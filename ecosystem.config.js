module.exports = {
  apps: [{
    name: "tmc-backend",
    script: "./server.js",
    instances: "max",
    exec_mode: "cluster",
    env: {
      NODE_ENV: "production",
      PORT: 13340 // Ensure matches Dockerfile EXPOSE and Cloud Run env
    }
  }]
};
