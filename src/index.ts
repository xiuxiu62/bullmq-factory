import { FlowProducer, Job, Queue, QueueEvents, Worker } from 'bullmq';
import IORedis from 'ioredis';
import ld from "lodash";

enum QueueEvent {
    Complete = "completed",
    Failure = "failed",
}

type QueueHooks<Message, Response> = {
    completion?: (job: Job<Message, Response, string>, response: Response) => void;
    failure?: (job: Job<Message, Response, string> | undefined, error: Error) => void;
}

export class QueueBundle<Message, Response> {
    private title: string;
    worker: Worker<Message, Response, string>;
    queue: Queue<Message, Response, string>;
    queue_events: QueueEvents;
    flow: FlowProducer;

    private messages: Array<Message>;
    private responses: Array<Response | Error>;

    constructor(
        title: string,
        worker: Worker<Message, Response, string>,
        queue: Queue<Message, Response, string>,
        queue_events: QueueEvents,
        flow: FlowProducer,
        hooks: QueueHooks<Message, Response>,
    ) {
        this.title = title;
        this.worker = worker;
        this.queue = queue;
        this.queue_events = queue_events;
        this.flow = flow;

        this.messages = [];
        this.responses = [];

        const completion_hook = (job: Job<Message, Response, string>, response: Response) => {
            hooks.completion?.(job, response);
            this.responses.push(response);
        };

        const failure_hook = (job: Job<Message, Response, string> | undefined, error: Error) => {
            hooks.failure?.(job, error);
            this.responses.push(error);
        };

        this.worker
            .on(QueueEvent.Complete, completion_hook)
            .on(QueueEvent.Failure, failure_hook);
    }

    add(message: Message): this {
        this.messages.push(message);

        return this;
    }

    add_many(messages: Array<Message>): this {
        messages.forEach(message => this.messages.push(message))

        return this;
    }

    async run(): Promise<Array<Response | Error>> {
        const children = this.messages
            .map(message => ({
                name: this.title,
                data: message,
                queueName: this.title
            }));
        const node = await this.flow.add({
            name: this.title,
            queueName: this.title,
            data: "example",
            children: children,
        });

        await node.job.waitUntilFinished(this.queue_events);

        return this.responses;
    }

    // async for_each_response(response_handler: (response: Response) => void): Promise<void> {
    //     return;
    // }

    // async wait_until_all_finished(): Promise<void> {
    //     for (const job of this.jobs) {
    //         const response = await job.waitUntilFinished(this.queue_events);
    //         // response
    //     }

    //     return;

    // }
}

type QueueBuilderProps<Message, Response> = {
    title?: string;
    processor?: (job: Job<Message, Response, string>) => Promise<Response>;
    hooks: QueueHooks<Message, Response>;
    on_complete?: (job: Job<Message, Response, string>, response: Response) => void;
    on_failure?: (job: Job<Message, Response, string> | undefined, error: Error) => void;
};

export class QueueBuilder<Message, Response> {
    private connection: IORedis;
    private props: QueueBuilderProps<Message, Response>;

    constructor(connection: IORedis) {
        this.connection = connection;
        this.props = {
            hooks: {}
        };
    }

    named(value: string): this {
        this.props.title = value;

        return this;
    }

    with_processor(value: (job: Job<Message, Response, string>) => Promise<Response>): this {
        this.props.processor = value;

        return this;
    }

    with_completion_hook(
        value: (job: Job<Message, Response, string>, response: Response) => void
    ): this {
        this.props.hooks.completion = value;

        return this;
    }

    with_failure_hook(
        value: (job: Job<Message, Response, string> | undefined, error: Error) => void
    ): this {
        this.props.hooks.failure = value;

        return this;
    }

    build(): QueueBundle<Message, Response> {
        if (!this.props.title)
            throw new Error('No title given for worker bundle');

        if (!this.props.processor)
            throw new Error('No job handler given for worker bundle');

        const queue = new Queue<Message, Response>(this.props.title, {
            connection: this.connection,
            sharedConnection: true,
            defaultJobOptions: {
                removeOnComplete: true,
                removeOnFail: { age: 1000 }, // Job TTL past failure of 1000ms
            },
        });

        const worker = new Worker<Message, Response>(this.props.title, this.props.processor, {
            connection: this.connection,
            sharedConnection: true,
            concurrency: 50,
        });

        const queue_events = new QueueEvents(this.props.title, { connection: this.connection });

        const flow = new FlowProducer();

        return new QueueBundle(this.props.title, worker, queue, queue_events, flow, this.props.hooks);
    }
}

export class QueueFactory {
    private connection: IORedis;

    constructor(host: string, port: number) {
        this.connection = new IORedis(port, host, { maxRetriesPerRequest: null });
        this.connection.on('ready', () => console.log(`Redis connected @ ${host}:${port}`));
    }

    new<Message, Response>(): QueueBuilder<Message, Response> {
        return new QueueBuilder(this.connection);
    }
}

const host = process.env.REDIS_HOST ?? 'localhost';
const port = process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379;
const queue_factory = new QueueFactory(host, port);

export default queue_factory;

//================================================================//  

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

(async () => {
    type Message = string;
    type Response = string;

    const job_count = 20;
    const messages = ld.range(job_count).map(i => `${i}`);
    const responses = await queue_factory
        .new<Message, Response>()
        .named("example")
        .with_processor(async job => {
            await sleep(500);

            return job.data;
        })
        .build()
        .add_many(messages)
        .run();

    console.log(responses);

    // console.assert(responses.length === job_count, "Not all jobs finished");

    // let responses: Array<Response | Error> = [];
    // const bundle = queue_factory
    //     .new<Message, Response>()
    //     .named("example")
    //     .with_processor(async job => {
    //         await sleep(500);

    //         return job.data;
    //     })
    //     .with_completion_hook((_job, response) => responses.push(response))
    //     // .with_failure_hook((job, error) => cons)
    //     .build();

    // const job_count = 20;
    // const messages = ld.range(job_count).map(i => `${i}`);
    // const responses = await bundle.add_many(messages).run();


    // await bundle.add_many(ld.range(job_count).map(i => `temp-${i + 1}`));
    // await bundle.add_many(  Array(100).fill(`temp-${++i}`));

    // await bundle.add_many(["xiu", "chad", "cloud"]);
    // await bundle.for_each_response(response => responses.push(response));
    // await bundle.wait_until_all_finished();


    // await sleep(1000);

    // responses.forEach(response => console.log("<response>", response));

    // const flow = new FlowProducer();
    // const children = ld.range(job_count)
    //     .map(i => ({
    //         name: "example",
    //         data: `temp-${i + 1}`,
    //         queueName: "example"
    //     }));
    // const node = await flow.add({
    //     name: "example",
    //     queueName: "example",
    //     data: "hello",
    //     children: children,
    // });

    // await node.job.waitUntilFinished(bundle.queue_events);
    // responses.forEach(response => console.log("<response>", response));
    // console.log(await node.job.isCompleted());

    // console.log(node);
})()
