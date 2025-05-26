import {
  Expand,
  FunctionReference,
  GenericActionCtx,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
} from "convex/server";
import { GenericId, v } from "convex/values";
import { api } from "../component/_generated/api";
import { StreamStatus } from "../component/schema";

export type StreamId = string & { __isStreamId: true };
export const StreamIdValidator = v.string();
export type StreamBody = {
  text: string;
  status: StreamStatus;
};

export type ChunkAppender = (text: string) => Promise<void>;
export type StreamWriter<A extends GenericActionCtx<GenericDataModel>> = (
  ctx: A,
  request: Request,
  streamId: StreamId,
  chunkAppender: ChunkAppender
) => Promise<void>;

export type ActionStreamWriter<A extends GenericActionCtx<GenericDataModel>> = (
  ctx: A,
  streamId: StreamId,
  chunkAppender: ChunkAppender
) => Promise<void>;

export type GetStreamOptions = {
  pollInterval?: number; // milliseconds, default 1000
  timeout?: number; // milliseconds, default 30000
};

// TODO -- make more flexible. # of bytes, etc?
const hasDelimeter = (text: string) => {
  return text.includes(".") || text.includes("!") || text.includes("?");
};

// TODO -- some sort of wrapper with easy ergonomics for working with LLMs?
export class PersistentTextStreaming {
  constructor(
    public component: UseApi<typeof api>,
    public options?: object
  ) {}

  /**
   * Create a new stream. This will return a stream ID that can be used
   * in an HTTP action to stream data back out to the client while also
   * permanently persisting the final stream in the database.
   *
   * @param ctx - A convex context capable of running mutations.
   * @returns The ID of the new stream.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const streamId = await streaming.createStream(ctx);
   * await streaming.stream(ctx, request, streamId, async (ctx, req, id, append) => {
   *   await append("Hello ");
   *   await append("World!");
   * });
   * ```
   */

  async createStream(ctx: RunMutationCtx): Promise<StreamId> {
    const id = await ctx.runMutation(this.component.lib.createStream);
    return id as StreamId;
  }

  /**
   * Get the body of a stream. This will return the full text of the stream
   * and the status of the stream.
   *
   * @param ctx - A convex context capable of running queries.
   * @param streamId - The ID of the stream to get the body of.
   * @returns The body of the stream and the status of the stream.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const { text, status } = await streaming.getStreamBody(ctx, streamId);
   * ```
   */
  async getStreamBody(
    ctx: RunQueryCtx,
    streamId: StreamId
  ): Promise<StreamBody> {
    const { text, status } = await ctx.runQuery(
      this.component.lib.getStreamText,
      { streamId }
    );
    return { text, status: status as StreamStatus };
  }

  /**
   * Inside an HTTP action, this will stream data back to the client while
   * also persisting the final stream in the database.
   *
   * @param ctx - A convex context capable of running actions.
   * @param request - The HTTP request object.
   * @param streamId - The ID of the stream.
   * @param streamWriter - A function that generates chunks and writes them
   * to the stream with the given `StreamWriter`.
   * @returns A promise that resolves to an HTTP response. You may need to adjust
   * the headers of this response for CORS, etc.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const streamId = await streaming.createStream(ctx);
   * const response = await streaming.stream(ctx, request, streamId, async (ctx, req, id, append) => {
   *   await append("Hello ");
   *   await append("World!");
   * });
   * ```
   */
  async httpStream<A extends GenericActionCtx<GenericDataModel>>(
    ctx: A,
    request: Request,
    streamId: StreamId,
    streamWriter: StreamWriter<A>
  ) {
    const streamState = await ctx.runQuery(this.component.lib.getStreamStatus, {
      streamId,
    });
    if (streamState !== "pending") {
      console.log("Stream was already started");
      return new Response("", {
        status: 205,
      });
    }
    // Create a TransformStream to handle streaming data
    const { readable, writable } = new TransformStream();
    let writer = writable.getWriter() as WritableStreamDefaultWriter<Uint8Array> | null;
    const textEncoder = new TextEncoder();
    let pending = "";

    const doStream = async () => {
      const chunkAppender: ChunkAppender = async (text) => {
        // write to this handler's response stream on every update
        if (writer) {
          try {
            await writer.write(textEncoder.encode(text));
          } catch (e) {
            console.error("Error writing to stream", e);
            console.error("Will skip writing to stream but continue database updates");
            writer = null;
          }
        }
        pending += text;
        // write to the database periodically, like at the end of sentences
        if (hasDelimeter(text)) {
          await this.addChunk(ctx, streamId, pending, false);
          pending = "";
        }
      };
      try {
        await streamWriter(ctx, request, streamId, chunkAppender);
      } catch (e) {
        await this.setStreamStatus(ctx, streamId, "error");
        if (writer) {
          await writer.close();
        }
        throw e;
      }

      // Success? Flush any last updates
      await this.addChunk(ctx, streamId, pending, true);

      if (writer) {
        await writer.close();
      }
    };

    // Kick off the streaming, but don't await it.
    void doStream();

    // Send the readable back to the browser
    return new Response(readable);
  }

  /**
   * Stream data to the database without returning HTTP chunks. This is useful
   * for background processing where you want to persist the stream but don't
   * need to stream the response back to a client.
   *
   * @param ctx - A convex context capable of running actions.
   * @param streamId - The ID of the stream.
   * @param streamWriter - A function that generates chunks and writes them
   * to the stream with the given `ActionStreamWriter`.
   * @returns A promise that resolves when the streaming is complete.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const streamId = await streaming.createStream(ctx);
   * await streaming.actionStream(ctx, streamId, async (ctx, id, append) => {
   *   await append("Hello ");
   *   await append("World!");
   * });
   * ```
   */
  async actionStream<A extends GenericActionCtx<GenericDataModel>>(
    ctx: A,
    streamId: StreamId,
    streamWriter: ActionStreamWriter<A>
  ) {
    const streamState = await ctx.runQuery(this.component.lib.getStreamStatus, {
      streamId,
    });
    if (streamState !== "pending") {
      throw new Error("Stream was already started");
    }

    let pending = "";

    const chunkAppender: ChunkAppender = async (text) => {
      pending += text;
      // write to the database periodically, like at the end of sentences
      if (hasDelimeter(text)) {
        await this.addChunk(ctx, streamId, pending, false);
        pending = "";
      }
    };

    try {
      await streamWriter(ctx, streamId, chunkAppender);
    } catch (e) {
      await this.setStreamStatus(ctx, streamId, "error");
      throw e;
    }

    // Success? Flush any last updates
    await this.addChunk(ctx, streamId, pending, true);
  }

  /**
   * Poll for stream chunks with configurable interval and timeout. This will
   * continuously check for updates to the stream until it's complete or times out.
   *
   * @param ctx - A convex context capable of running queries.
   * @param streamId - The ID of the stream to poll.
   * @param options - Configuration for polling behavior.
   * @returns A promise that resolves to the final stream body when complete.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const result = await streaming.getStream(ctx, streamId, {
   *   pollInterval: 500,
   *   timeout: 60000
   * });
   * console.log(result.text, result.status);
   * ```
   */
  async getStream(
    ctx: RunQueryCtx,
    streamId: StreamId,
    options: GetStreamOptions = {}
  ): Promise<StreamBody> {
    const { pollInterval = 1000, timeout = 30000 } = options;
    const startTime = Date.now();

    while (true) {
      const streamBody = await this.getStreamBody(ctx, streamId);
      
      // If stream is complete (done, error, or timeout), return immediately
      if (streamBody.status === "done" || streamBody.status === "error" || streamBody.status === "timeout") {
        return streamBody;
      }

      // Check if we've exceeded the timeout
      if (Date.now() - startTime > timeout) {
        throw new Error(`Stream polling timed out after ${timeout}ms`);
      }

      // Wait for the poll interval before checking again
      await new Promise(resolve => setTimeout(resolve, pollInterval));
    }
  }

  // Internal helper -- add a chunk to the stream.
  private async addChunk(
    ctx: RunMutationCtx,
    streamId: StreamId,
    text: string,
    final: boolean
  ) {
    await ctx.runMutation(this.component.lib.addChunk, {
      streamId,
      text,
      final,
    });
  }

  // Internal helper -- set the status of a stream.
  private async setStreamStatus(
    ctx: RunMutationCtx,
    streamId: StreamId,
    status: StreamStatus
  ) {
    await ctx.runMutation(this.component.lib.setStreamStatus, {
      streamId,
      status,
    });
  }
}

/* Type utils follow */

type RunQueryCtx = {
  runQuery: GenericQueryCtx<GenericDataModel>["runQuery"];
};
type RunMutationCtx = {
  runMutation: GenericMutationCtx<GenericDataModel>["runMutation"];
};

export type OpaqueIds<T> = T extends GenericId<infer _T> | string
  ? string
  : T extends (infer U)[]
    ? OpaqueIds<U>[]
    : T extends ArrayBuffer
      ? ArrayBuffer
      : T extends object
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T;

export type UseApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    "public",
    infer FArgs,
    infer FReturnType,
    infer FComponentPath
  >
    ? FunctionReference<
        FType,
        "internal",
        OpaqueIds<FArgs>,
        OpaqueIds<FReturnType>,
        FComponentPath
      >
    : UseApi<API[mod]>;
}>;
