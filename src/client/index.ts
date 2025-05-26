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

export type ActionStreamWriter<A extends GenericActionCtx<GenericDataModel>> = (
  ctx: A,
  streamId: StreamId,
) => Promise<void>;

export type ChunkAppender = (chunk: string) => Promise<void>;

export type GetStreamOptions = {
  pollInterval?: number; // milliseconds, default 1000
  timeout?: number; // milliseconds, default 30000
};

// TODO -- make more flexible. # of bytes, etc?
const hasDelimeter = (chunk: string) => {
  return chunk.includes(".") || chunk.includes("!") || chunk.includes("?");
};

// TODO -- some sort of wrapper with easy ergonomics for working with LLMs?
export class PersistentTextStreaming {
  constructor(
    public component: UseApi<typeof api>,
    public options: GetStreamOptions = {
      pollInterval: 1000,
      timeout: 30000,
    }
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
   * Get the body of a stream. This will return the full chunks of the stream
   * and the status of the stream.
   *
   * @param ctx - A convex context capable of running queries.
   * @param streamId - The ID of the stream to get the body of.
   * @returns The body of the stream and the status of the stream.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const { chunks, status } = await streaming.getStreamBody(ctx, streamId);
   * ```
   */
  async getStreamBody(
    ctx: RunQueryCtx,
    streamId: StreamId
  ) {
    const result = await ctx.runQuery(
      this.component.lib.getStreamChunks,
      { streamId }
    );
    if (!result) {
      throw new Error("Stream not found");
    }
    return result;
  }

  /**
   * Inside an HTTP action, this will stream data back to the client while
   * also persisting the final stream in the database.
   *
   * @param ctx - A convex context capable of running actions.
   * @param request - The HTTP request object.
   * @param streamId - The ID of the stream.
   * @returns A promise that resolves to an HTTP response. You may need to adjust
   * the headers of this response for CORS, etc.
   * @example
   * ```ts
   * const streaming = new PersistentTextStreaming(api);
   * const streamId = await streaming.createStream(ctx);
   * const response = await streaming.stream(ctx, request, streamId);
   * ```
   */
  async stream<A extends GenericActionCtx<GenericDataModel>>(
    ctx: A,
    request: Request,
    streamId: StreamId,
  ) {
    const { pollInterval = 1000, timeout = 30000 } = this.options;
    const startTime = Date.now();

    // Get initial chunks
    const initialChunks = await ctx.runQuery(this.component.lib.getStreamChunks, {
      streamId,
    });
    
    let lastCreatedAt = initialChunks?.chunks[initialChunks.chunks.length - 1]?._creationTime;

    const streamState = await ctx.runQuery(this.component.lib.getStreamStatus, {
      streamId,
    });
    
    if (streamState !== "pending" && streamState !== "streaming") {
      return new Response("", {
        status: 205,
      });
    }

    // Create a TransformStream to handle streaming data
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const textEncoder = new TextEncoder();

    const doStream = async () => {
      try {
        // First, send initial chunks if any
        if (initialChunks?.chunks) {
          for (const chunk of initialChunks.chunks) {
            await writer.write(textEncoder.encode(chunk.chunk));
          }
        }

        // Poll for new chunks
        while (true) {
          const currentTime = Date.now();
          
          // Check timeout
          if (currentTime - startTime > timeout) {
            console.log("Stream timeout reached");
            break;
          }

          // Get new chunks since last poll
          const result = await ctx.runQuery(this.component.lib.getNewStreamChunks, {
            streamId,
            lastCreatedAt,
          });

          // Send new chunks
          for (const chunk of result.chunks) {
            await writer.write(textEncoder.encode(chunk.chunk));
            lastCreatedAt = chunk._creationTime;
          }

          // Check if stream is complete
          if (result.status === "done" || result.status === "error" || result.status === "timeout") {
            console.log(`Stream completed with status: ${result.status}`);
            break;
          }

          // Wait for next poll
          await new Promise(resolve => setTimeout(resolve, pollInterval));
        }
      } catch (e) {
        console.error("Error in stream polling:", e);
      } finally {
        await writer.close();
      }
    };

    // Start streaming in background
    void doStream();

    // Return the readable stream
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
   * await streaming.actionStream(ctx, streamId, async (ctx, id) => {
   *   // Your streaming logic here
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

    const chunkAppender: ChunkAppender = async (chunk: string) => {
      pending += chunk;
      // write to the database periodically, like at the end of sentences
      if (hasDelimeter(chunk)) {
        await this.addChunk(ctx, streamId, pending, false);
        pending = "";
      }
    };

    try {
      await streamWriter(ctx, streamId);
    } catch (e) {
      await this.setStreamStatus(ctx, streamId, "error");
      throw e;
    }

    // Success? Flush any last updates
    await this.addChunk(ctx, streamId, pending, true);
  }

  // Internal helper -- add a chunk to the stream.
  private async addChunk(
    ctx: RunMutationCtx,
    streamId: StreamId,
    chunk: string,
    final: boolean
  ) {
    await ctx.runMutation(this.component.lib.addChunk, {
      streamId,
      chunk,
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
