const express = require('express');
const cors = require('cors');
const compression = require('compression');
const { BigQuery } = require('@google-cloud/bigquery');
const { BigQueryReadClient } = require('@google-cloud/bigquery-storage');
const { Storage } = require('@google-cloud/storage');
const avro = require('avsc');

const moment = require('moment');
const multer = require('multer');

const upload = multer();

const app = express();
const PORT = process.env.PORT || 5000;

// Increase timeout for long-running BigQuery streams (10 minutes)
const TIMEOUT_MS = 10 * 60 * 1000;

// Middleware
app.use(cors());
app.use(compression({
    filter: (req, res) => {
        // Force compression for heatmap routes even if Content-Type is x-ndjson
        if (req.path.includes('generate_heatmap') ||
            req.path.includes('/api/heatmap')) {
            return true;
        }
        // Fallback to standard filter for other routes
        // This ensures standard types (json, html) are still compressed
        return compression.filter(req, res);
    }
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get('/', (req, res) => {
    res.send('TMC Express Backend is running');
});

// Google Cloud Clients
const PROJECT_ID = 'tmc-dashboards';
const bigquery = new BigQuery({ projectId: PROJECT_ID });
const bqReadClient = new BigQueryReadClient({ projectId: PROJECT_ID });
const storage = new Storage({ projectId: PROJECT_ID });

const BUCKET_NAME = "bkt-heatmap4f-camloc";
const PREFIX = "tuned/";

// --- STANDARD SQL HELPERS ---

async function executeStandardQueryStream(query, onData, onEnd, onError) {
    try {
        const stream = bigquery.createQueryStream({ query });
        stream.on('data', onData);
        stream.on('end', onEnd);
        stream.on('error', onError);
    } catch (e) {
        onError(e);
    }
}

const TZ_OFFSETS = {
    'EST': '-05:00',
    'EDT': '-04:00',
    'CST': '-06:00',
    'CDT': '-05:00',
    'MST': '-07:00',
    'MDT': '-06:00',
    'PST': '-08:00',
    'PDT': '-07:00',
    'HST': '-10:00',
    'HDT': '-09:00'
};

// --- ROUTES ---

// GET /get_camera_locations
app.get('/get_camera_locations', async (req, res) => {
    try {
        const { state, route, start_mile, end_mile } = req.query;

        if (!route || start_mile === undefined || end_mile === undefined) {
            return res.status(400).json({ error: "Required params: route (string), start_mile (int), end_mile (int)" });
        }
        const sMile = parseInt(start_mile);
        const eMile = parseInt(end_mile);
        const stateParam = state || 'IN';

        if (stateParam !== 'IN') {
            return res.json({
                state: stateParam,
                route,
                start_mile: sMile,
                end_mile: eMile,
                locations: []
            });
        }

        if (sMile > eMile) {
            return res.status(400).json({ error: "start_mile must be <= end_mile" });
        }

        const query = `
        SELECT DISTINCT CAST(mile AS FLOAT64) AS mile
        FROM \`tmc-dashboards.Heatmap.camera_parsed\`
        WHERE route = replace(@route, '-', '')
          AND mile >= @start_mile
          AND mile <= @end_mile
        ORDER BY mile ASC
        `;

        const options = {
            query: query,
            params: { route: route, start_mile: sMile, end_mile: eMile }
        };

        const [rows] = await bigquery.query(options);
        const locations = rows.map(row => parseFloat(row.mile));

        res.json({
            state: stateParam,
            route,
            start_mile: sMile,
            end_mile: eMile,
            locations
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

// GET /get_exit_lines
app.get('/get_exit_lines', async (req, res) => {
    try {
        const { state, route, start_mile, end_mile } = req.query;

        if (!route || start_mile === undefined || end_mile === undefined) {
            return res.status(400).json({ error: "Required params: route, start_mile, end_mile" });
        }
        const sMile = parseFloat(start_mile);
        const eMile = parseFloat(end_mile);
        const stateParam = state || 'AL';

        // We'll fetch for both directions if they exist, or the frontend can filter.
        // The table has interstate_dir like 'I-10 E'
        const query = `
        SELECT interstate_dir, CAST(milepost AS FLOAT64) AS milepost, exit
        FROM \`tmc-dashboards.Heatmap.exit_lines\`
        WHERE state = @state
          AND (interstate_dir LIKE @route_pattern_1 OR interstate_dir LIKE @route_pattern_2)
          AND milepost >= @min_mile
          AND milepost <= @max_mile
        ORDER BY milepost ASC
        `;

        const options = {
            query: query,
            params: {
                state: stateParam,
                route_pattern_1: `${route} %`,
                route_pattern_2: `${route.replace('-', '')} %`,
                min_mile: Math.min(sMile, eMile),
                max_mile: Math.max(sMile, eMile)
            }
        };

        const [rows] = await bigquery.query(options);
        res.json(rows);

    } catch (e) {
        console.error("Error in /get_exit_lines:", e);
        res.status(500).json({ error: e.message });
    }
});

// CAR: POST /generate_heatmap_car
app.post('/generate_heatmap_car', upload.none(), (req, res) => {
    const { start_date, end_date, direction, start_mm, end_mm, state, timezone } = req.body;
    handleCarRequest(state || 'IN', direction, start_date, end_date, parseFloat(start_mm), parseFloat(end_mm), res, timezone);
});

// TRUCK: POST /generate_heatmap_truck
app.post('/generate_heatmap_truck', upload.none(), (req, res) => {
    const { start_date, end_date, direction, start_mm, end_mm, state, timezone } = req.body;
    handleTruckRequest(state || 'IN', direction, start_date, end_date, parseFloat(start_mm), parseFloat(end_mm), res, timezone);
});

// GET endpoints matching heatmap-dashboard-frontend-master params
app.get('/api/heatmap/getMiles/:state/:roadName/:startDate/:endDate/:startmm/:endmm/:timezone', (req, res) => {
    const { state, roadName, startDate, endDate, startmm, endmm, timezone } = req.params;
    handleCarRequest(state, roadName, startDate, endDate, parseFloat(startmm), parseFloat(endmm), res, timezone);
});
app.get('/api/heatmap/getMiles/:state/:roadName/:startDate/:endDate/:startmm/:endmm', (req, res) => {
    const { state, roadName, startDate, endDate, startmm, endmm } = req.params;
    handleCarRequest(state, roadName, startDate, endDate, parseFloat(startmm), parseFloat(endmm), res, 'EST');
});

app.get('/api/heatmap/getMiles_truck/:state/:roadName/:startDate/:endDate/:startmm/:endmm/:timezone', (req, res) => {
    const { state, roadName, startDate, endDate, startmm, endmm, timezone } = req.params;
    handleTruckRequest(state, roadName, startDate, endDate, parseFloat(startmm), parseFloat(endmm), res, timezone);
});
app.get('/api/heatmap/getMiles_truck/:state/:roadName/:startDate/:endDate/:startmm/:endmm', (req, res) => {
    const { state, roadName, startDate, endDate, startmm, endmm } = req.params;
    handleTruckRequest(state, roadName, startDate, endDate, parseFloat(startmm), parseFloat(endmm), res, 'EST');
});

// --- HANDLERS ---

const formatTimestamp = (ts) => {
    if (!ts) return ts;
    // If it's just a date (YYYY-MM-DD), append time
    if (/^\d{4}-\d{2}-\d{2}$/.test(ts)) {
        return ts + 'T00:00:00Z';
    }
    // Otherwise, replace space with T and ensure it ends with Z
    let formatted = ts.replace(' ', 'T');
    if (!formatted.endsWith('Z')) {
        formatted += 'Z';
    }
    return formatted;
};

async function handleCarRequest(state, roadName, startDate, endDate, startmm, endmm, res, timezone = 'EST') {
    console.log(`[Car] Aggregated Streaming Request: [${state}] ${roadName} ${startDate} to ${endDate} (${startmm} to ${endmm}) TZ: ${timezone}`);

    const tzOffset = TZ_OFFSETS[timezone] || '-05:00';

    // Calculate Dynamic aggregation based on mileage range
    const deltaMM = Math.abs(endmm - startmm);
    let mmPrecision = 1; // 0.1 mile
    let binSeconds = 60; // 1 minute buckets by default

    if (deltaMM > 100) {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 180;  // 1 minute buckets
    } else if (deltaMM > 50) {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 120;  // 1 minute buckets
    } else {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 60;  // 1 minute buckets
    }

    const query = `
    SELECT
       ROUND(mm, ${mmPrecision}) AS mm, 
       UNIX_SECONDS(
     TIMESTAMP_SECONDS(
       DIV(
         UNIX_SECONDS(TIMESTAMP(DATETIME(timestamp, '${tzOffset}'))), 
         ${binSeconds}
       ) * ${binSeconds}
     )
   ) AS bin,
       ROUND(AVG(medSpeed) * 0.62137119) as mph
    FROM
      smart_poly.movement_agg a
    LEFT JOIN
      shapefiles.smart_polys p ON p.id = a.id
    WHERE
        timestamp >= TIMESTAMP('${startDate}', '${tzOffset}') AND timestamp < TIMESTAMP('${endDate}', '${tzOffset}')
        AND state = '${state}' AND route ='${roadName}' AND mm BETWEEN ${startmm} AND ${endmm}
    GROUP BY bin, mm
    ORDER BY bin, mm ASC
    `;


    try {
        // Set proper headers for streaming response
        res.setHeader('Content-Type', 'application/x-ndjson');
        res.setHeader('Transfer-Encoding', 'chunked');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('X-Content-Type-Options', 'nosniff');


        // Set timeout for this response
        res.setTimeout(TIMEOUT_MS);

        const stream = bigquery.createQueryStream({ query });

        let jobRef = null;
        stream.on('job', (job) => {
            jobRef = job;
        });

        stream.on('data', (row) => {
            const outRow = {
                mm: row.mm,
                bin: row.bin,
                mph: row.mph,
                event_type: 'car',
                mmStep: mmPrecision === 0 ? 1.0 : 0.1,
                binStep: binSeconds
            };
            res.write(JSON.stringify(outRow) + '\n');
        });

        stream.on('end', async () => {
            console.log('[Car] Stream completed successfully');

            if (jobRef) {
                try {
                    const [metadata] = await jobRef.getMetadata();
                    const stats = metadata.statistics;
                    // query.totalBytesProcessed or totalBytesBilled
                    const bytesVal = parseInt(stats.totalBytesBilled || stats.totalBytesProcessed || '0');
                    const costVal = (bytesVal / 1099511627776) * 6.25; // using $6.25 per TiB

                    const metaRow = {
                        meta: true,
                        bytes: bytesVal,
                        cost: costVal
                    };
                    res.write(JSON.stringify(metaRow) + '\n');
                } catch (e) {
                    console.error("Error fetching job stats", e);
                }
            }

            // Ensure all data is flushed before closing
            setImmediate(() => {
                res.end();
            });
        });

        stream.on('error', (err) => {
            console.error("Car Query Stream Error:", err);
            if (!res.headersSent) {
                res.status(500).json({ error: err.message });
            } else {
                // If headers already sent, just end the stream properly
                res.end();
            }
        });

    } catch (e) {
        console.error("handleCarRequest Error:", e);
        if (!res.headersSent) res.status(500).json({ error: e.message });
        else res.end();
    }
}

async function handleTruckRequest(state, roadName, startDate, endDate, startmm, endmm, res, timezone = 'EST') {
    console.log(`[Truck] Aggregated Streaming Request: [${state}] ${roadName} ${startDate} to ${endDate} (${startmm} to ${endmm}) TZ: ${timezone}`);

    const tzOffset = TZ_OFFSETS[timezone] || '-05:00';

    // Calculate Dynamic aggregation based on mileage range
    const deltaMM = Math.abs(endmm - startmm);
    let mmPrecision = 1; // 0.1 mile
    let binSeconds = 60; // 1 minute buckets

    if (deltaMM > 100) {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 180;  // 1 minute buckets
    } else if (deltaMM > 50) {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 120;  // 1 minute buckets
    } else {
        mmPrecision = 1;   // 0.1 mile buckets
        binSeconds = 60;  // 1 minute buckets
    }

    const query = `
    SELECT
       ROUND(mm, ${mmPrecision}) AS mm, 
       UNIX_SECONDS(
     TIMESTAMP_SECONDS(
       DIV(
         UNIX_SECONDS(TIMESTAMP(DATETIME(timestamp, '${tzOffset}'))), 
         ${binSeconds}
       ) * ${binSeconds}
     )
   ) AS bin,
       ROUND(AVG(medSpeed) * 0.62137119) as mph
    FROM
      smart_poly.truck_agg a
    LEFT JOIN
      shapefiles.smart_polys p ON p.id = a.id
    WHERE
     timestamp >= TIMESTAMP('${startDate}', '${tzOffset}') AND timestamp < TIMESTAMP('${endDate}', '${tzOffset}')
        AND state = '${state}' AND route ='${roadName}' AND mm BETWEEN ${startmm} AND ${endmm}
    GROUP BY bin, mm
    ORDER BY bin, mm ASC
    `;


    try {
        // Set proper headers for streaming response
        res.setHeader('Content-Type', 'application/x-ndjson');
        res.setHeader('Transfer-Encoding', 'chunked');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('X-Content-Type-Options', 'nosniff');


        // Set timeout for this response
        res.setTimeout(TIMEOUT_MS);

        const stream = bigquery.createQueryStream({ query });

        let jobRef = null;
        stream.on('job', (job) => {
            jobRef = job;
        });

        stream.on('data', (row) => {
            const outRow = {
                mm: row.mm,
                bin: row.bin,
                mph: row.mph,
                event_type: 'truck',
                mmStep: mmPrecision === 0 ? 1.0 : 0.1,
                binStep: binSeconds
            };
            res.write(JSON.stringify(outRow) + '\n');
        });

        stream.on('end', async () => {
            console.log('[Truck] Stream completed successfully');

            if (jobRef) {
                try {
                    const [metadata] = await jobRef.getMetadata();
                    const stats = metadata.statistics;
                    // query.totalBytesProcessed or totalBytesBilled
                    const bytesVal = parseInt(stats.totalBytesBilled || stats.totalBytesProcessed || '0');
                    const costVal = (bytesVal / 1099511627776) * 6.25; // using $6.25 per TiB

                    const metaRow = {
                        meta: true,
                        bytes: bytesVal,
                        cost: costVal
                    };
                    res.write(JSON.stringify(metaRow) + '\n');
                } catch (e) {
                    console.error("Error fetching job stats", e);
                }
            }

            // Ensure all data is flushed before closing
            setImmediate(() => {
                res.end();
            });
        });

        stream.on('error', (err) => {
            console.error("Truck Query Stream Error:", err);
            if (!res.headersSent) {
                res.status(500).json({ error: err.message });
            } else {
                // If headers already sent, just end the stream properly
                res.end();
            }
        });

    } catch (e) {
        console.error("handleTruckRequest Error:", e);
        if (!res.headersSent) res.status(500).json({ error: e.message });
        else res.end();
    }
}

async function getPolyIds(state, roadName, startmm, endmm) {
    const s = Math.min(startmm, endmm);
    const e = Math.max(startmm, endmm);
    const tableReference = `projects/${PROJECT_ID}/datasets/shapefiles/tables/smart_polys`;
    const readOptions = {
        selectedFields: ['id', 'mm'],
        rowRestriction: `state="${state}" AND route = "${roadName}" AND mm BETWEEN ${s} AND ${e}`
    };

    const poly_ids_to_mm = {};
    await streamReadRows(tableReference, readOptions, (decodedData) => {
        poly_ids_to_mm[decodedData.id] = decodedData.mm;
    });
    return poly_ids_to_mm;
}

async function streamReadRows(tableReference, readOptions, onData) {
    const parent = `projects/${PROJECT_ID}`;
    const request = {
        parent,
        readSession: {
            table: tableReference,
            dataFormat: 'AVRO',
            readOptions,
        },
    };

    const [session] = await bqReadClient.createReadSession(request);
    const schema = JSON.parse(session.avroSchema.schema);
    const avroType = avro.Type.forSchema(schema);

    const readPromises = session.streams.map(streamItem => {
        return new Promise((resolve, reject) => {
            const stream = bqReadClient.readRows({ readStream: streamItem.name });
            stream.on('error', reject);
            stream.on('data', data => {
                let pos = 0;
                while (pos < data.avroRows.serializedBinaryRows.length) {
                    try {
                        const { value, offset } = avroType.decode(data.avroRows.serializedBinaryRows, pos);
                        onData(value);
                        pos = offset;
                        if (offset <= 0) break;
                    } catch (err) {
                        break;
                    }
                }
            });
            stream.on('end', resolve);
        });
    });

    await Promise.all(readPromises);
}

// Events are tricky - they rely on a different query structure (accel/decel).
// For now, retaining a basic SQL placeholder for events or removing if not critical. 
// But the user asked for full implementation. The reference `BigQuery.js` implements braking via `getBraking` which uses standard SQL.
// So we should keep standard SQL for events.

const EVENTS_QUERY_TEMPLATE = `
WITH polys AS (
  SELECT
    id,
    route AS direction,
    mm,
    geog,
    heading
  FROM \`tmc-dashboards.shapefiles.smart_polys\`
  WHERE state = '{state}'
    AND route = '{direction}'
    AND mtfcc = 'S1100'
    AND mm BETWEEN {start_mm} AND {end_mm}
    AND (
      active_date IS NULL
      OR deactive_date IS NULL
      OR TIMESTAMP('{start_date}') BETWEEN active_date AND deactive_date
    )
),

events AS (
  SELECT
    journeyId,
    bin_a1 AS local_ts,
    accel_g,
    heading_a1,
    geog_a1 AS geog,
    CASE
      WHEN accel_g >= {accel} THEN 'accel'
      WHEN accel_g <= {decel} THEN 'decel'
      ELSE 'IGNORE'
    END AS event_type
  FROM \`tmc-dashboards.streetlight.event_accel_interpolated\`
  WHERE bin_a1 >= TIMESTAMP(DATETIME('{start_date} 00:00:00'), '{tzOffset}')
    AND bin_a1 < TIMESTAMP(DATETIME(DATE_ADD(DATE('{end_date}'), INTERVAL 1 DAY), '00:00:00'), '{tzOffset}')

  UNION ALL

  SELECT
    journeyId,
    bin_a1 AS local_ts,
    accel_g,
    heading_a1,
    geog_a1 AS geog,
    CASE
      WHEN accel_g >= {accel} THEN 'accel'
      WHEN accel_g <= {decel} THEN 'decel'
      ELSE 'IGNORE'
    END AS event_type
  FROM \`tmc-dashboards.streetlight.event_accel_interpolated_sample_nov9-16_2025\`
  WHERE bin_a1 >= TIMESTAMP(DATETIME('{start_date} 00:00:00'), '{tzOffset}')
    AND bin_a1 < TIMESTAMP(DATETIME(DATE_ADD(DATE('{end_date}'), INTERVAL 1 DAY), '00:00:00'), '{tzOffset}')
),

filtered AS (
  SELECT *
  FROM events
  WHERE event_type IN ('accel','decel')
),

joined AS (
  SELECT
    p.direction,
    p.mm,
    f.local_ts,
    f.accel_g,
    f.event_type
  FROM filtered f
  JOIN polys p
    ON ST_INTERSECTS(f.geog, p.geog)
    AND (
         ABS(f.heading_a1 - p.heading) < 15
         OR ABS(f.heading_a1 - p.heading) > 345
        )
),

binned AS (
  SELECT
    direction,
    ROUND(mm, 1) AS mm,
    local_ts,
    accel_g,
    event_type
  FROM joined
)

SELECT
  direction,
  UNIX_SECONDS(
     TIMESTAMP_SECONDS(
       DIV(
         UNIX_SECONDS(TIMESTAMP(DATETIME(local_ts, '{tzOffset}'))), 
         {binSeconds}
       ) * {binSeconds}
     )
   ) AS bin, 
  mm, 
  accel_g as speed, 
  event_type 
FROM binned
`;

function formatQuery(template, params) {
    let query = template;
    for (const key in params) {
        const regex = new RegExp(`\\{${key}\\}`, 'g');
        query = query.replace(regex, params[key]);
    }
    return query;
}

app.post('/generate_heatmap_events', upload.none(), async (req, res) => {
    try {
        const { start_date, end_date, direction, start_mm, end_mm, accel, decel, state } = req.body;
        const accelVal = accel ? parseFloat(accel) : 0.25;
        const decelVal = decel ? parseFloat(decel) : -0.25;
        const stateParam = state || 'IN';

        // Calculate Dynamic aggregation based on mileage range
        const sMM = parseFloat(start_mm);
        const eMM = parseFloat(end_mm);
        const deltaMM = Math.abs(eMM - sMM);
        let binSeconds = 60;

        if (deltaMM > 100) {
            binSeconds = 60;
        } else if (deltaMM > 50) {
            binSeconds = 60;
        } else {
            binSeconds = 60;
        }

        const { timezone } = req.body;
        const tzOffset = TZ_OFFSETS[timezone || 'EST'] || '-05:00';

        const query = formatQuery(EVENTS_QUERY_TEMPLATE, {
            start_date, end_date, direction, start_mm, end_mm,
            accel: accelVal, decel: decelVal, binSeconds: binSeconds, state: stateParam,
            tzOffset: tzOffset
        });

        // Use standard BQ for events as it's a complex spatial join query, not a straight table read
        // const [rows] = await bigquery.query(query);

        const [job] = await bigquery.createQueryJob({ query });
        const [rows] = await job.getQueryResults();

        const [metadata] = await job.getMetadata();
        const stats = metadata.statistics;
        const bytesVal = parseInt(stats.totalBytesBilled || stats.totalBytesProcessed || '0');
        const costVal = (bytesVal / 1099511627776) * 6.25;

        rows.push({
            meta: true,
            bytes: bytesVal,
            cost: costVal
        });

        res.json(rows);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.post('/generate_heatmap_vizzion', upload.none(), async (req, res) => {
    try {
        const { start_date, end_date, direction, start_mm, end_mm, state, timezone } = req.body;
        const stateParam = state || 'IN';
        const tzOffset = TZ_OFFSETS[timezone] || '-05:00';

        const query = `
        SELECT
            UNIX_SECONDS(time) as bin,
            state,
            route as direction,
            mm
        FROM \`tmc-dashboards.vizzion.vizzion_drives\`
        WHERE state = @state
          AND mm BETWEEN @min_mm AND @max_mm
          AND TRIM(route) = @direction
          AND time >= TIMESTAMP('${start_date}', '${tzOffset}') AND time < TIMESTAMP('${end_date}', '${tzOffset}')
        ORDER BY time ASC
        `;

        const options = {
            query: query,
            params: {
                state: stateParam,
                min_mm: Math.min(parseFloat(start_mm), parseFloat(end_mm)),
                max_mm: Math.max(parseFloat(start_mm), parseFloat(end_mm)),
                direction: direction,
                start_date: start_date,
                end_date: moment(end_date).add(1, 'day').format('YYYY-MM-DD'),
                tzOffset: tzOffset
            }
        };

        const [job] = await bigquery.createQueryJob(options);
        const [rows] = await job.getQueryResults();

        const [metadata] = await job.getMetadata();
        const stats = metadata.statistics;
        const bytesVal = parseInt(stats.totalBytesBilled || stats.totalBytesProcessed || '0');
        const costVal = (bytesVal / 1099511627776) * 6.25;

        // Map rows to include event_type
        const mappedRows = rows.map(r => ({
            ...r,
            event_type: 'vizzion'
        }));

        mappedRows.push({
            meta: true,
            bytes: bytesVal,
            cost: costVal
        });

        res.json(mappedRows);
    } catch (e) {
        console.error("Vizzion Query Error:", e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/get-images', async (req, res) => {
    const { timestamp, road: proad, mile, state } = req.query;

    if (!timestamp || !proad || !mile) {
        return res.status(400).json({ error: "timestamp, road, mile required" });
    }

    if (state && state !== 'IN') {
        return res.json({
            timestamp,
            road: proad,
            mile,
            state,
            images: { cam1: "", cam2: "", cam3: "" }
        });
    }

    const road = proad.replace("I-", "").padStart(3, '0');
    let mileFormatted = "";
    try {
        const parts = String(mile).split(".");
        mileFormatted = `${parts[0].padStart(3, '0')}-${parts[1] || '0'}`;
        if (parts.length < 2) throw new Error("mile must be like 7.5");
    } catch (e) {
        return res.status(400).json({ error: "mile must be like 7.5" });
    }

    const baseTime = moment(timestamp, "YYYYMMDDTHHmmss");
    let foundData = null;

    for (let diff = 0; diff <= 120; diff++) {
        const t1 = moment(baseTime).subtract(diff, 'seconds');
        const r1 = await checkAllCameras(t1, road, mileFormatted);
        if (r1.found) {
            foundData = r1.data;
            break;
        }

        if (diff > 0) {
            const t2 = moment(baseTime).add(diff, 'seconds');
            const r2 = await checkAllCameras(t2, road, mileFormatted);
            if (r2.found) {
                foundData = r2.data;
                break;
            }
        }
    }

    if (!foundData) {
        foundData = { cam1: "", cam2: "", cam3: "" };
    }

    res.json({
        timestamp,
        road: proad,
        mile,
        images: foundData
    });
});

async function checkAllCameras(timeObj, road, mileFormatted) {
    const ts = timeObj.format("YYYYMMDDTHHmmss");
    const [cam1, cam2, cam3] = await Promise.all([
        findImage(ts, road, mileFormatted, 1),
        findImage(ts, road, mileFormatted, 2),
        findImage(ts, road, mileFormatted, 3)
    ]);

    return {
        found: (cam1 !== "" || cam2 !== "" || cam3 !== ""),
        data: { cam1, cam2, cam3 }
    };
}

async function findImage(timestamp, road, mileFormatted, camNo) {
    const filename = `${timestamp}_1-${road}-${mileFormatted}-_-_-cam-${camNo}.jpg`;
    const blobPath = PREFIX + filename;
    const bucket = storage.bucket(BUCKET_NAME);
    const file = bucket.file(blobPath);

    try {
        const [exists] = await file.exists();
        if (exists) {
            return await generateSignedUrl(file);
        }
    } catch (e) {
        console.error("GCS Error Ignored:", e.message);
    }
    return "";
}


async function generateSignedUrl(file) {
    const options = {
        version: 'v4',
        action: 'read',
        expires: Date.now() + 3600 * 1000 // 1 hour
    };
    const [url] = await file.getSignedUrl(options);
    return url;
}

const server = app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

// Set server timeouts for long-running streams
server.timeout = TIMEOUT_MS;
server.keepAliveTimeout = TIMEOUT_MS;
server.headersTimeout = TIMEOUT_MS + 1000; // Slightly longer than keepAliveTimeout

process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
