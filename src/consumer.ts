import {
  AckPolicy,
  Codec,
  connect,
  ConsumerInfo,
  ConsumerMessages,
  JetStreamClient,
  JetStreamManager,
  JsMsg,
  JSONCodec,
  NatsConnection,
  NatsError,
  StorageType,
  StreamInfo,
} from "nats";
import { filter, Observable, Subject } from "rxjs";

const PARTITION_COUNT = Number(process.env.PARTITION_COUNT || "1");
const PARTITION = Number(process.env.PARTITION || "0");

let jsm: JetStreamManager;
let js: JetStreamClient;
let jc: Codec<unknown>;
let startedAt = Date.now();
let shutdownCalls = 0;
let nc: NatsConnection | null = null;
let consumerMessages: ConsumerMessages | null = null;

function elapsed(): number {
  return Date.now() - startedAt;
}

async function doesStreamExist(streamName: string): Promise<boolean> {
  try {
    await jsm.streams.info(streamName);

    return true;
  } catch (error) {
    if (error instanceof NatsError && error.code === "404") {
      return false;
    }

    throw error;
  }
}

async function doesConsumerExist(streamName: string, consumerName: string) {
  try {
    await jsm.consumers.info(streamName, consumerName);

    return true;
  } catch (error) {
    if (error instanceof NatsError && error.code === "404") {
      return false;
    }

    throw error;
  }
}

async function main() {
  nc = await connect({ servers: "nats://localhost:4225" });

  jsm = await nc.jetstreamManager();
  js = nc.jetstream();
  jc = JSONCodec();

  // TODO: confirm if stream updates are OK with partitions and stuff.
  const streamExists = await doesStreamExist("players");
  let streamInfo: StreamInfo;
  if (!streamExists) {
    streamInfo = await jsm.streams.add({
      name: "players",
      subjects: ["player.*"],
      subject_transform: {
        src: "player.*",
        dest: `player.{{partition(${PARTITION_COUNT},1)}}`,
      },
      storage: StorageType.Memory,
    });
    console.debug(elapsed(), `Stream created`, streamInfo);
  } else {
    streamInfo = await jsm.streams.update("players", {
      subjects: ["player.*"],
      subject_transform: {
        src: "player.*",
        dest: `player.{{partition(${PARTITION_COUNT},1)}}`,
      },
    });
    console.debug(elapsed(), `Stream updated`, streamInfo);
  }

  // TODO: confirm if consumer updates are OK with partitions and stuff.
  const consumerExists = await doesConsumerExist(
    "players",
    `players-partition:${PARTITION}`
  );
  let consumerInfo: ConsumerInfo;
  if (!consumerExists) {
    consumerInfo = await jsm.consumers.add("players", {
      ack_policy: AckPolicy.Explicit,
      ack_wait: 10 * 5_000_000_000,
      max_ack_pending: 15,
      durable_name: `players-partition:${PARTITION}`,
      filter_subject: `player.${PARTITION}`,
    });
    console.debug(elapsed(), `Consumer created`, consumerInfo);
  } else {
    consumerInfo = await jsm.consumers.update(
      "players",
      `players-partition:${PARTITION}`,
      {
        ack_wait: 10 * 5_000_000_000,
        max_ack_pending: 15,
        filter_subject: `player.${PARTITION}`,
      }
    );
    console.debug(elapsed(), `Consumer updated`, consumerInfo);
  }

  const consumer = await js.consumers.get(
    consumerInfo.stream_name,
    consumerInfo.name
  );
  consumerMessages = await consumer.consume({ max_messages: 3 });

  console.debug(elapsed(), `Waiting for messages`);
  const messages$ = new Subject<JsMsg>();
  const messageBatches$ = messages$.pipe(
    bufferTimeAndCount(1_000, 10),
    filter((batch) => batch.length > 0)
  );

  messageBatches$.subscribe((batch) => {
    console.debug(
      elapsed(),
      `Received batch of messages, amount ${batch.length}`
    );
    batch.forEach((m) => {
      m.ack();
    });
  });

  for await (const m of consumerMessages) {
    console.debug(
      elapsed(),
      `Received message`,
      m.subject,
      m.seq,
      jc.decode(m.data)
    );
    messages$.next(m);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGQUIT", shutdown);

main().catch((error) => {
  console.error(error);
});

async function shutdown() {
  shutdownCalls += 1;

  if (shutdownCalls > 1) {
    console.debug(`Received second shutdown signal, forcing exit`);
    process.exit(1);
  }

  console.debug(`Received shutdown signal`);

  if (consumerMessages) {
    console.debug("Closing consumer messages");
    await consumerMessages.close();
    console.debug("Consumer messages closed");
  }

  if (nc) {
    console.debug("Closing connection");
    await nc?.close();
    console.debug("Connection closed");
  }
}

function bufferTimeAndCount<T>(
  time: number,
  count: number
  // TODO: implement deduplication, `keep` and `override` strategies
  // deduplicateBy?: (left: T, right: T) => boolean
): (source: Observable<T>) => Observable<T[]> {
  return (source: Observable<T>) =>
    new Observable<T[]>((observer) => {
      let buffer: T[] = [];
      let timeout: NodeJS.Timeout | null = null;

      const resetTimeout = () => {
        if (!timeout) {
          return;
        }

        clearTimeout(timeout);
        timeout = null;
      };

      const emit = () => {
        observer.next(buffer);
        buffer = [];
        resetTimeout();
      };

      const scheduleEmit = () => {
        if (timeout) {
          return;
        }

        timeout = setTimeout(emit, time);
      };

      const subscription = source.subscribe({
        next(value) {
          buffer.push(value);

          if (buffer.length === count) {
            emit();
            return;
          }

          scheduleEmit();
        },
        complete() {
          emit();
          observer.complete();
        },
        error(error) {
          observer.error(error);
        },
      });

      return () => {
        subscription.unsubscribe();
        resetTimeout();
      };
    });
}
