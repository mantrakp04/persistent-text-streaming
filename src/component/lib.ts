import { v, Value } from "convex/values";
import { internalMutation, mutation, query } from "./_generated/server";
import { streamStatusValidator } from "./schema";
import { ExpressionOrValue } from "convex/server";
import { Doc } from "./_generated/dataModel";

// Create a new stream with zero chunks.
export const createStream = mutation({
  args: {},
  handler: async (ctx) => {
    const streamId = await ctx.db.insert("streams", {
      status: "pending",
    });
    return streamId;
  },
});

// Add a chunk to a stream.
// If final is true, set the stream to done.
// Can only be done on streams which are pending or streaming.
export const addChunk = mutation({
  args: {
    streamId: v.id("streams"),
    chunk: v.string(),
    final: v.boolean(),
  },
  handler: async (ctx, args) => {
    const stream = await ctx.db.get(args.streamId);
    if (!stream) {
      throw new Error("Stream not found");
    }
    if (stream.status === "pending") {
      await ctx.db.patch(args.streamId, {
        status: "streaming",
      });
    } else if (stream.status !== "streaming") {
      throw new Error("Stream is not streaming; did it timeout?");
    }
    await ctx.db.insert("chunks", {
      streamId: args.streamId,
      chunk: args.chunk,
    });
    if (args.final) {
      await ctx.db.patch(args.streamId, {
        status: "done",
      });
    }
  },
});

// Set the status of a stream.
// Can only be done on streams which are pending or streaming.
export const setStreamStatus = mutation({
  args: {
    streamId: v.id("streams"),
    status: v.union(
      v.literal("pending"),
      v.literal("streaming"),
      v.literal("done"),
      v.literal("error"),
      v.literal("timeout")
    ),
  },
  handler: async (ctx, args) => {
    const stream = await ctx.db.get(args.streamId);
    if (!stream) {
      throw new Error("Stream not found");
    }
    if (stream.status !== "pending" && stream.status !== "streaming") {
      console.log(
        "Stream is already finalized; ignoring status change",
        stream
      );
      return;
    }
    await ctx.db.patch(args.streamId, {
      status: args.status,
    });
  },
});

// Get the status of a stream.
export const getStreamStatus = query({
  args: {
    streamId: v.id("streams"),
  },
  returns: streamStatusValidator,
  handler: async (ctx, args) => {
    const stream = await ctx.db.get(args.streamId);
    return stream?.status ?? "error";
  },
});

export const getStreamChunks = query({
  args: {
    streamId: v.id("streams"),
  },
  handler: async (ctx, args) => {
    const stream = await ctx.db.get(args.streamId);
    if (!stream) {
      throw new Error("Stream not found");
    }
    
    if (stream.status !== "pending") {
      const chunks = await ctx.db
        .query("chunks")
        .withIndex("byStream", (q) => q.eq("streamId", args.streamId))
        .collect();

      return {
        chunks: chunks as Doc<"chunks">[],
        status: stream.status,
      };
    }
  },
});

export const getNewStreamChunks = query({
  args: {
    streamId: v.id("streams"),
    lastCreatedAt: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const stream = await ctx.db.get(args.streamId);
    if (!stream) {
      throw new Error("Stream not found");
    }
    const chunks = await ctx.db
      .query("chunks")
      .withIndex("byStream", (q) => q.eq("streamId", args.streamId))
      .filter((q) => q.gt("_creationTime", args.lastCreatedAt ?? 0 as ExpressionOrValue<Value>))
      .collect();
    return {
      chunks,
      status: stream.status,
    };
  },
});

const EXPIRATION_TIME = 20 * 60 * 1000; // 20 minutes in milliseconds
const BATCH_SIZE = 100;

// If the last chunk of a stream was added more than 20 minutes ago,
// set the stream to timeout. The action feeding it has to be dead.
export const cleanupExpiredStreams = internalMutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    const pendingStreams = await ctx.db
      .query("streams")
      .withIndex("byStatus", (q) => q.eq("status", "pending"))
      .take(BATCH_SIZE);
    const streamingStreams = await ctx.db
      .query("streams")
      .withIndex("byStatus", (q) => q.eq("status", "streaming"))
      .take(BATCH_SIZE);

    for (const stream of [...pendingStreams, ...streamingStreams]) {
      if (now - stream._creationTime > EXPIRATION_TIME) {
        console.log("Cleaning up expired stream", stream._id);
        await ctx.db.patch(stream._id, {
          status: "timeout",
        });
      }
    }
  },
});
