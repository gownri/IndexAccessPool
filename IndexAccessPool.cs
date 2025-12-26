#define UNFORESEENTHROWNFALLBACK
//#define EXCEPTIONSAFE
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace IndexFriendlyCollections;

public readonly struct IndexAccessPool<T> : IDisposable
    where T : class
{
    internal static bool IsSupported { get; } = Unsafe.SizeOf<(int, int)>() == 8 ? true : throw new NotSupportedException();

    private const int fallbackArraySize = 1;
    private const int fallbackThreshold = 40000;

    private readonly (T?[]? items, (int count, int conflictionCount) counts)[] pool;
    private readonly (GCHandle[]? items, (int count, int conflictionCount) counts)[] lohPool;

    public readonly bool IsNull
        => this.pool is null;
    public readonly int Length
        => this.pool.Length + this.lohPool.Length;
    public readonly int LOHBorderIndex
        => this.pool.Length;
    public IndexAccessPool(int length, int lohBorderIndex = -1, int defaultPoolSize = 1, scoped ReadOnlySpan<int> poolSizeOfIndexes = default)
    {
        var lohLength = length - lohBorderIndex;
        if ((long)length < (long)(uint)lohLength)
        {
            this.pool = Init<T?>(length, defaultPoolSize, poolSizeOfIndexes);
            this.lohPool = [];
        }
        else
        {
            this.pool = Init<T?>(lohBorderIndex, defaultPoolSize, poolSizeOfIndexes);
            if ((uint)lohBorderIndex < (uint)poolSizeOfIndexes.Length)
                poolSizeOfIndexes = poolSizeOfIndexes[lohBorderIndex..];
            else
                poolSizeOfIndexes = default;
            this.lohPool = Init<GCHandle>(lohLength, defaultPoolSize, poolSizeOfIndexes);
        }

        static (U[]?, (int, int))[] Init<U>(int length, int defaultPoolSize, ReadOnlySpan<int> sizeOfIndex)
        {
            if (length <= 0)
                return [];
            var pool = new (U[]?, (int, int))[length];
            int size;
            for (var i = 0; i < pool.Length; i++)
            {
                if ((uint)i < (uint)sizeOfIndex.Length)
                    size = sizeOfIndex[i];
                else
                    size = defaultPoolSize;

                pool[i] = (size > 0 ? new U[size] : [], (0, 0));
            }
            return pool;
        }
    }

    public readonly bool TryRent(int index, [MaybeNullWhen(false)] scoped out T item, scoped ReadOnlySpan<int> alternateIndexes = default)
    {
        var p = this.pool;
        var loh = this.lohPool;
        var idx = index;
        var altRead = 0;
        var conflicted = false;
        var extend = false;
        do
        {
            if ((uint)idx < (uint)p.Length)
            {
                ref var location = ref p[idx];
                T?[]? pool = default;
#if EXCEPTIONSAFE
                try
                {
#endif
                pool = Interlocked.Exchange(ref location.items, null!);
                if (pool is null)
                {
                    conflicted = true;
#if UNFORESEENTHROWNFALLBACK
                    UnforeseenThrownFallback(ref location, this.pool);
#endif
                }
                else
                {
                    for (var i = location.counts.count - 1; (uint)i < (uint)pool.Length; --i)
                    {
                        var temp = pool[i];
                        if (temp is not null)
                        {
                            location.counts = (i, 0);
                            pool[i] = default;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, pool);
#endif
                            item = temp;
                            return true;
                        }
                    }
#if !EXCEPTIONSAFE
                    Volatile.Write(ref location.items, pool);
#endif
                }
#if EXCEPTIONSAFE
                }
                finally
                {
                    if (pool is not null)
                        Volatile.Write(ref location.items, pool);
                }
#endif
            }

            idx -= p.Length;
            if ((uint)idx < (uint)loh.Length)
            {
                ref var location = ref loh[idx];
                GCHandle[]? lohPool = default!;
#if EXCEPTIONSAFE
                try
                {
#endif
                lohPool = Interlocked.Exchange(ref location.items, null!);
                if (lohPool is null)
                { 
                    conflicted = true;
#if UNFORESEENTHROWNFALLBACK
                    UnforeseenThrownFallback(ref location, this.lohPool);
#endif
                }
                else
                {
                    for (var i = location.counts.count - 1; (uint)i < (uint)lohPool.Length; --i)
                    {
                        if (!lohPool[i].IsAllocated)
                            continue;

                        var temp = lohPool[i].Target;
                        if (temp is not null)
                        {
                            location.counts = (i, 0);
                            lohPool[i].Target = null;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            item = Unsafe.As<T>(temp);
                            return true;
                        }
                    }
#if !EXCEPTIONSAFE
                    Volatile.Write(ref location.items, lohPool);
#endif
                }
#if EXCEPTIONSAFE
                }
                finally
                {
                    if (lohPool is not null)
                        Volatile.Write(ref location.items, lohPool);
                }
#endif
            }

            if ((uint)altRead < (uint)alternateIndexes.Length)
            {
                idx = alternateIndexes[altRead];
                altRead++;
            }
            else
            {
                if (conflicted)
                {
                    altRead = 0;
                    idx = index;
                    conflicted = false;
                    if (extend)
                        Thread.Sleep(0);
                    else
                        extend = Thread.Yield();
                    continue;
                }
                item = default;
                return false;
            }

        } while (true);
    }

    public readonly bool Return(int index, T item, scoped ReadOnlySpan<int> alternateIndexes = default)
    {
        var p = this.pool;
        var loh = this.lohPool;
        var idx = index;
        var altRead = 0;
        var conflicted = false;
        var extend = false;
        if (item is null)
            return true;
        do
        {
            if ((uint)idx < (uint)p.Length)
            {
                ref var location = ref p[idx];
                T?[]? pool = default!;
#if EXCEPTIONSAFE
                try
                {
#endif
                pool = Interlocked.Exchange(ref location.items, null);
                if (pool is null)
                {
                    conflicted = true;
#if UNFORESEENTHROWNFALLBACK
                    UnforeseenThrownFallback(ref location, this.pool);
#endif
                }
                else
                {
                    for (var i = location.counts.count; (uint)i < (uint)pool.Length; ++i)
                    {
                        if (pool[i] is null)
                        {
                            location.counts = (i + 1, 0);
                            pool[i] = item;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, pool);
#endif
                            return true;
                        }
                    }
#if !EXCEPTIONSAFE
                    Volatile.Write(ref location.items, pool);
#endif
                }
            }
#if EXCEPTIONSAFE
                finally
                {
                    if (pool is not null)
                        Volatile.Write(ref location.items, pool);
                }
            }
#endif
            idx -= p.Length;
            if ((uint)idx < (uint)loh.Length)
            {
                ref var location = ref loh[idx];
                GCHandle[]? lohPool = default!;
#if EXCEPTIONSAFE
                try
                {
#endif
                lohPool = Interlocked.Exchange(ref location.items, null);
                if (lohPool is null)
                {
                    conflicted = true;
#if UNFORESEENTHROWNFALLBACK
                    UnforeseenThrownFallback(ref location, this.lohPool);
#endif
                }
                else
                {
                    for (var i = location.counts.count; (uint)i < (uint)lohPool.Length; ++i)
                    {
                        if (!lohPool[i].IsAllocated)
                        {
                            lohPool[i] = GCHandle.Alloc(item, GCHandleType.Weak);
                            location.counts = (i + 1, 0);
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }

                        if (lohPool[i].Target is null)
                        {
                            lohPool[i].Target = item;
                            location.counts = (i + 1, 0);
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }
                    }

                    for (var i = lohPool.Length - 1; (uint)i < (uint)lohPool.Length; --i)
                    {
                        if (!lohPool[i].IsAllocated)
                        {
                            lohPool[i] = GCHandle.Alloc(item, GCHandleType.Weak);
                            location.counts.conflictionCount = 0;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }

                        if (lohPool[i].Target is null)
                        {
                            lohPool[i].Target = item;
                            location.counts.conflictionCount = 0;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }
                    }
#if !EXCEPTIONSAFE
                    Volatile.Write(ref location.items, lohPool);
#endif
                }
#if EXCEPTIONSAFE
                }
                finally
                {
                    if (lohPool is not null)
                        Volatile.Write(ref location.items, lohPool);
                }
#endif
            }

            if ((uint)altRead < (uint)alternateIndexes.Length)
            {
                idx = alternateIndexes[altRead];
                altRead++;
            }
            else
            {
                if (conflicted)
                {
                    altRead = 0;
                    idx = index;
                    conflicted = false;
                    if (extend)
                        Thread.Sleep(0);
                    else
                        extend = Thread.Yield();
                    continue;
                }
                return false;
            }

        } while (true);


    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void UnforeseenThrownFallback<U>(scoped ref (U[]? items, (int count, int conflictions) counts) location, object lockObj, [CallerLineNumber] int line = 0)
    {
        if (Interlocked.Increment(ref location.counts.conflictions) < fallbackThreshold)
            return;
        Debug.WriteLine($"fallback by (count, conflictions) : [{location.counts}] in {line}");
        lock(lockObj)
        {
            if(location.counts.conflictions < fallbackThreshold)
                return;
            var array = new U[fallbackArraySize];
            location.items = array;
            location.counts.count = 0;
            Volatile.Write(ref location.counts.conflictions, 0);
        }
    }
    public readonly void Dispose()
    {
        var count = 0;
        foreach (ref var pool in this.pool.AsSpan())
        {
            T?[]? items = default;
            try
            {
                while (true)
                {
                    items = Interlocked.Exchange(ref pool.items, null!);
                    if (items is null)
                    {
                        if (count++ > fallbackThreshold)
                            break;
                        Thread.Sleep(0);
                        continue;
                    }

                    foreach (ref var item in items.AsSpan())
                        item = null;
                    pool.counts = default;
                    break;
                }
            }
            finally
            {
                if (items is not null)
                    Volatile.Write(ref pool.items, items);
            }
        }

        count = 0;
        foreach (ref var pool in this.lohPool.AsSpan())
        {
            GCHandle[]? items = default;
            try
            {
                while (true)
                {
                    items = Interlocked.Exchange(ref pool.items, null!);
                    if (items is null)
                    {
                        if (count++ > fallbackThreshold)
                            break;
                        Thread.Sleep(0);
                        continue;
                    }
                    foreach (ref var gcHandle in items.AsSpan())
                        if (gcHandle.IsAllocated)
                            gcHandle.Free();
                    pool.counts = default;
                    break;
                }
            }
            finally
            {
                if (items is not null)
                    Volatile.Write(ref pool.items, items);
            }
        }
    }

    public readonly string DEBUG__PoolsDump()
    {
        var sb = new System.Text.StringBuilder();

        sb.AppendLine($"{nameof(IndexAccessPool<T>)} : {this.Length}");
        if (this.IsNull)
            return sb.ToString();
        var offset = 0;
        sb.Append("pool : ")
            .Append(this.pool.Length)
        .AppendLine();
        {
            var pool = this.pool;
            for (var i = 0; i < pool.Length; ++i)
            {
                sb.Append("\tindexOf:")
                    .Append(i + offset)
                    .Append(", ");
                var items = pool[i].items;
                if (items is null)
                    sb.Append("items:null");
                else
                {
                    sb.Append("items:[");
                    foreach (var item in items)
                    {
                        sb.Append(
                            item
                        )
                        .Append(", ");
                    }
                    sb.Append("] in ")
                    .Append(items.Length);
                }
                sb.Append(", count:")
                    .Append(pool[i].counts.count)
                    .Append(" || conflicts:")
                    .Append(pool[i].counts.conflictionCount)
                .AppendLine();
            }
        }

        offset = this.pool.Length;
        sb.Append("lohPool : ")
            .Append(this.lohPool.Length)
        .AppendLine();
        {
            var pool = this.lohPool;
            for (var i = 0; i < pool.Length; ++i)
            {
                sb.Append("\tindexOf:")
                    .Append(i + offset)
                .Append(", ");
                var items = pool[i].items;
                if (items is null)
                    sb.Append("items is null");
                else
                {
                    sb.Append("items:[");
                    foreach (var item in items)
                    {
                        sb.Append(
                            item.IsAllocated ? item.Target : "null"
                        )
                        .Append(", ");
                    }
                    sb.Append("] in ")
                    .Append(items.Length);
                }
                sb.Append(", count:")
                    .Append(pool[i].counts.count)
                    .Append(" || conflicts:")
                    .Append(pool[i].counts.conflictionCount)
                .AppendLine();
            }
        }

        return sb.ToString();
    }
}

/*
test code

using IndexFriendlyCollections;
using System.Threading;
var pool = new IndexAccessPool<object>(32, 20, 4);

{
    Parallel.For(0, 10000, v =>
{
    if (!pool.TryRent(v % 31, out var a))
        a = (object)v;
    System.Threading.Thread.Sleep(0);
    pool.Return(v % 31, a);
}
);
        Console.WriteLine(pool.DEBUG__DumpPools());
}

*/