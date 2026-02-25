import { buildApp } from "./app";

const app = buildApp();

const port = Number(app.env.API_PORT || 4000);

app.listen({ port, host: "0.0.0.0" })
  .then(() => {
    app.log.info(`API listening on ${port}`);
  })
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });

const shutdownSignals = ["SIGTERM", "SIGINT"] as const;
shutdownSignals.forEach((signal) => {
  process.on(signal, async () => {
    app.log.info({ signal }, "Shutting down API");
    try {
      await app.close();
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, "Failed to shutdown cleanly");
      process.exit(1);
    }
  });
});
