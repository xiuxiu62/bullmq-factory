import { Job, Queue, Worker } from "bullmq";
import IORedis from "ioredis";

export class QueueBundle<Message, Response> {
  private title: string;
  worker: Worker<Message, Response, string>;
  queue: Queue<Message, Response, string>;

  constructor(
    title: string,
    worker: Worker<Message, Response, string>,
    queue: Queue<Message, Response, string>,

  ) {
    this.title = title;
    this.worker = worker;
    this.queue = queue;
  }

  async add(message: Message, run_immediately: boolean = false): Promise<Job<Message, Response, string>> {
    return await this.queue.add(this.title, message, { lifo: run_immediately });
  }

  async add_many(messages: Array<{ name: string, data: Message }>): Promise<Array<Job<Message, Response, string>>> {
    return await this.queue.addBulk(messages);
  }

  async job_count(): Promise<number> {
    return (await this.queue.getJobs()).length;
  }

  async run(): Promise<void> {
    await this.worker.run();
  }
}

type QueueBuilderProps<Message, Response> = {
  title?: string;
  processor?: (job: Job<Message, Response, string>) => Promise<Response>;
  on_complete?: (job: Job<Message, Response, string>, response: Response) => void;
  on_failure?: (job: Job<Message, Response, string> | undefined, error: Error) => void;
}

export class QueueBuilder<Message, Response> {
  private connection: IORedis;
  private props: QueueBuilderProps<Message, Response>;

  constructor(connection: IORedis) {
    this.connection = connection;
    this.props = {};
  }

  title(value: string): this {
    this.props.title = value;

    return this;
  }

  processor(value: (job: Job<Message, Response, string>) => Promise<Response>): this {
    this.props.processor = value;

    return this;
  }

  on_complete(value: (job: Job<Message, Response, string>, response: Response) => void): this {
    this.props.on_complete = value;

    return this;
  }

  on_failure(value: (job: Job<Message, Response, string> | undefined, error: Error) => void): this {
    this.props.on_failure = value;

    return this;
  }

  build(): QueueBundle<Message, Response> {
    if (!this.props.title)
      throw new Error("No title given for worker bundle");

    if (!this.props.processor)
      throw new Error("No job handler given for worker bundle");

    const queue = new Queue<Message, Response>(
      this.props.title,
      {
        connection: this.connection,
        sharedConnection: true,
        defaultJobOptions: {
          removeOnComplete: true,
          removeOnFail: { age: 1000 }, // Job TTL past failure of 1000ms
        }
      });

    queue.on("waiting", (job: Job) => {
      console.log("here");
    });

    const worker = new Worker<Message, Response>(
      this.props.title,
      this.props.processor,
      {
        connection: this.connection,
        sharedConnection: true,
      });

    if (this.props.on_complete)
      worker.on("completed", this.props.on_complete);

    if (this.props.on_failure)
      worker.on("failed", this.props.on_failure);

    return new QueueBundle(this.props.title, worker, queue);
  }
}

export class QueueFactory {
  connection: IORedis;

  constructor(host: string, port: number) {
    this.connection = new IORedis(port, host, { maxRetriesPerRequest: null });
    this.connection.on("ready", () => console.log(`Redis connected @ ${host}:${port}`));
  }

  new<Message, Response>(): QueueBuilder<Message, Response> {
    return new QueueBuilder(this.connection);
  }
}

const queue_factory = new QueueFactory("localhost", 6379);
export default queue_factory;

/* ======================================================================
=========================================================================
====================================================================== */

type UserData = {
  name: string
}
type UserResponse = string;
type UserJob = Job<UserData, UserResponse, string>;


(async () => {
  try {
    async function job_handler(job: UserJob): Promise<string> {
      console.log(`hello ${job.data.name}`);

      return job.data.name;
    }

    const user_bundle =
      queue_factory.new<UserData, string>()
        .title("user")
        .processor(job_handler)
        .on_complete((_job, name) => console.log("Done saying hi to ", name))
        .on_failure((job, _err) => console.log("Failed to say hi to", job?.data.name, ":("))
        .build();

    await user_bundle.add({ name: "xiuxiu" });

    const queue_name = user_bundle.queue.name;
    console.log(`<${queue_name}> jobs:`, await user_bundle.job_count());
  } catch (err) {
    handle_error(err);
  }
})();

function handle_error(err: any): void {
  let message: string | null = null;
  if (typeof err === "string")
    message = err;

  if (err instanceof Error)
    message = err.message;

  if (message)
    console.log("Error", message);

  throw err;
}
