import { connect, JSONCodec } from "nats";

async function main() {
  const nc = await connect({ servers: "nats://localhost:4225" });
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
  const jc = JSONCodec();

  const playerIds = Array.from({ length: 10 }, (_, i) => i);
  const numbers = Array.from({ length: 3 }, (_, i) => i);

  await Promise.all(
    playerIds.map(async (id) => {
      await Promise.all(
        numbers.map(async (number) => {
          await js.publish(
            `player.${id}`,
            jc.encode({ id, name: `Player ${id}`, number })
          );
          console.log(`Published message ${number} for player ${id}`);
        })
      );
    })
  );

  await nc.close();
}

main().catch((error) => {
  console.error(error);
});
