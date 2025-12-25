using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Runtime.CompilerServices;

namespace IndexFriendlyCollections;

internal readonly struct IndexAccessPool<T> : IDisposable
    where T : class
{
    private const int sizeSelector = int.MinValue;
    private readonly (T?[]? items, int count)[] pool;
    private readonly (GCHandle[] items, int count)[] lohPool;

    internal readonly bool IsNull
        => this.pool is null;
    internal IndexAccessPool(int maxIndexSize, int lohBorderIndex = 0, int defaultPoolSize = 1, ReadOnlySpan<int> initialPoolLengthsOfIndex = default)
    {
        int size;
        if (maxIndexSize <= lohBorderIndex)
            this.pool = [];
        else
        {
            var poolOfIndex = new (T?[]?, int)[lohBorderIndex];
            for (var i = 0; i < poolOfIndex.Length; i++)
            {
                if ((uint)i < (uint)initialPoolLengthsOfIndex.Length)
                    size = (byte)initialPoolLengthsOfIndex[i];
                else
                    size = defaultPoolSize;

                poolOfIndex[i] = (size > 0 ? new T[size] : [], 0);
            }

            this.pool = poolOfIndex;
        }

        maxIndexSize -= lohBorderIndex;
        if (maxIndexSize <= 0)
        {
            this.lohPool = [];
            return;
        }

        var lohPool = new (GCHandle[], int)[maxIndexSize];
        if ((uint)maxIndexSize < (uint)initialPoolLengthsOfIndex.Length)
            initialPoolLengthsOfIndex = initialPoolLengthsOfIndex[maxIndexSize..];

        for (var i = 0; i < lohPool.Length; i++)
        {
            if ((uint)i < (uint)initialPoolLengthsOfIndex.Length)
                size = (byte)initialPoolLengthsOfIndex[i];
            else
                size = defaultPoolSize;

            lohPool[i] = (size > 0 ? new GCHandle[size] : [], 0);
        }

        this.lohPool = lohPool;
    }

    internal readonly bool TryRent(int index, [MaybeNullWhen(false)] out T item, ReadOnlySpan<int> alternateIndexes = default)
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
                    conflicted = true;
                else
                {
                    for (var i = location.count - 1; (uint)i < (uint)pool.Length; --i)
                    {
                        var temp = pool[i];
                        if (temp is not null)
                        {
                            location.count = i;
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

            idx = index - p.Length;
            if ((uint)idx < (uint)loh.Length)
            {
                ref var location = ref loh[idx];
                GCHandle[] lohPool = default!;
#if EXCEPTIONSAFE
                try
                {
#endif
                    lohPool = Interlocked.Exchange(ref location.items, null!);
                if (lohPool is null)
                    conflicted = true;
                else
                {
                    for (var i = location.count - 1; (uint)i < (uint)lohPool.Length; --i)
                    {
                        if (!lohPool[i].IsAllocated)
                            continue;

                        var temp = lohPool[i].Target;
                        if (temp is not null)
                        {
                            location.count = i;
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

    internal readonly bool Return(int index, T item, ReadOnlySpan<int> alternateIndexes = default)
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
                pool = Interlocked.Exchange(ref location.items, null!);
                if (pool is null)
                    conflicted = true;
                else
                {
                    for (var i = location.count; (uint)i < (uint)pool.Length; ++i)
                    {
                        if (pool[i] is null)
                        {
                            location.count = i + 1;
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
                GCHandle[] lohPool = default!;
#if EXCEPTIONSAFE
                try
                {
#endif
                lohPool = Interlocked.Exchange(ref location.items, null!);
                if (lohPool is null)
                    conflicted = true;
                else
                {
                    for (var i = location.count; (uint)i < (uint)lohPool.Length; ++i)
                    {
                        if (!lohPool[i].IsAllocated)
                        {
                            lohPool[i] = GCHandle.Alloc(item, GCHandleType.Weak);
                            location.count = i + 1;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }

                        if (lohPool[i].Target is null)
                        {
                            lohPool[i].Target = item;
                            location.count = i + 1;
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
                            location.count = i + 1;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }

                        if (lohPool[i].Target is null)
                        {
                            lohPool[i].Target = item;
                            location.count = i + 1;
#if !EXCEPTIONSAFE
                            Volatile.Write(ref location.items, lohPool);
#endif
                            return true;
                        }
                    }
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
                altRead = 0;
                if (conflicted)
                {
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
    public readonly void Dispose()
    {
        foreach (ref var pool in this.pool.AsSpan())
        {
            T?[]? items = default;
            try
            {
                items = Interlocked.Exchange(ref pool.items, null!);
                while (items is null)
                {
                    Thread.Sleep(0);
                    items = Interlocked.Exchange(ref pool.items, null!);
                }
                foreach (ref var item in items.AsSpan())
                    item = null;
                pool.count = 0;
            }
            finally
            {
                if(items is not null)
                    Volatile.Write(ref pool.items, items);
            }
        }

        foreach (ref var pool in this.lohPool.AsSpan())
        {
            GCHandle[]? items = default;
            try
            {
                items = Interlocked.Exchange(ref pool.items, null!);
                while (items is null)
                {
                    Thread.Sleep(0);
                    items = Interlocked.Exchange(ref pool.items, null!);
                }
                foreach (ref var gcHandle in items.AsSpan())
                    if (gcHandle.IsAllocated)
                        gcHandle.Free();
                pool.count = 0;
            }
            finally
            {
                if (items is not null)
                    Volatile.Write(ref pool.items, items);
            }
        }
    }

}

