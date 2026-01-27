# Use specific version for stability and security
FROM node:20-alpine

# Set environment variables for production
ENV NODE_ENV=production
ENV PORT=13340

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./

# Install only production dependencies
# Using npm install because package-lock.json might be out of sync with package.json
RUN npm install --omit=dev

# Install PM2 globally
RUN npm install pm2 -g

# Bundle app source
COPY . .

# Expose the port the app runs on
EXPOSE 13340

# Start the application using PM2
CMD [ "pm2-runtime", "start", "ecosystem.config.js" ]
