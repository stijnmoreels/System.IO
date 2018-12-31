namespace System.IO

open System
open System.IO

type MemoryFlag =
    | AutoOverflowToDisk
    | OnlyInMemory
    | OnlyToDisk

type private ThresholdSize =
    | FiftyMegabytes = 52_428_800
    | OneHundredMegabytes = 104_857_600
    | TwoHundredMegabytes = 209_715_200
    | TwoHundredFiftyMegabytes = 262_144_000

/// <summary>
/// Stream representation that automatically overflows to disk when the length of the wrapped stream is greater than the specified threshold size.
/// </summary>
type VirtualStream (?thresholdSize, ?flag, ?dataStream, ?forAsync) as x =
    inherit Stream ()
    let thresholdSize = defaultArg thresholdSize (VirtualStream.thresholdSizeMax)
    let memoryStatus = defaultArg flag AutoOverflowToDisk
    let forAsync = defaultArg forAsync false
    let mutable isDisposed = false
    let mutable isInMemory = memoryStatus <> OnlyToDisk

    let dataStream = 
        dataStream
        |> Option.defaultWith (fun () -> 
            if memoryStatus = OnlyToDisk 
            then VirtualStream.createPersistentStream forAsync
            else new MemoryStream () :> Stream)

    let whenNotDisposed f =
        if isDisposed || x.UnderlayingStream = null
        then raise (ObjectDisposedException "VirtualStream")
        else f ()

    let overflowToDisk l f =
        whenNotDisposed <| fun () ->
            if memoryStatus = AutoOverflowToDisk
               && isInMemory
               && l > thresholdSize
            then let persistent = VirtualStream.createPersistentStream forAsync
                 persistent.SetLength x.UnderlayingStream.Length
                 x.UnderlayingStream.Position <- 0L
                 x.UnderlayingStream.CopyTo persistent
                 x.UnderlayingStream <- persistent
                 isInMemory <- false
        f ()

    let cleanUp () =
        if isDisposed then ()
        isDisposed <- true
        if x.UnderlayingStream = null then ()
        x.UnderlayingStream.Close ()
        match x.UnderlayingStream with
        | :? FileStream as fs -> File.Delete fs.Name
        | _ -> ()
        x.UnderlayingStream <- null

    member val private UnderlayingStream : Stream = dataStream with get, set

    static member private createPersistentStream forAsync =
        let guid = Guid.NewGuid()
        let options = FileOptions.DeleteOnClose ||| FileOptions.SequentialScan
        let options = if forAsync then options ||| FileOptions.Asynchronous else options

        let fs = new FileStream(Path.Combine(Path.GetTempPath(), "VST", guid.ToString() + ".tmp"),
                                FileMode.Create,
                                FileAccess.ReadWrite,
                                FileShare.Read,
                                VirtualStream.defaultBufferSize,
                                options)
        File.SetAttributes(fs.Name, FileAttributes.Temporary ||| FileAttributes.NotContentIndexed)
        fs :> Stream

    static member private defaultBufferSize = 8192
    static member private thresholdSizeMax = int64 ThresholdSize.FiftyMegabytes

    /// <summary>
    /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
    /// </summary>
    static member Create (expectedSize, thresholdSize, ?forAsync) =
        let forAsync = defaultArg forAsync false
        if expectedSize > thresholdSize
        then new VirtualStream (thresholdSize, OnlyToDisk, VirtualStream.createPersistentStream forAsync, forAsync)
        else new VirtualStream (thresholdSize, AutoOverflowToDisk, new MemoryStream (VirtualStream.defaultBufferSize), forAsync)

    /// <summary>
    /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
    /// </summary>
    static member Create (expectedSize, ?forAsync) =
        let expectedSize = 
            if expectedSize < 0L then VirtualStream.thresholdSizeMax 
            else expectedSize

        VirtualStream.Create (expectedSize, VirtualStream.thresholdSizeMax, defaultArg forAsync false)

    /// <summary>
    /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
    /// </summary>
    static member Create (?forAsync) =
        VirtualStream.Create (VirtualStream.thresholdSizeMax, defaultArg forAsync false)

    /// <summary>
    /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
    /// </summary>
    static member Create (dataStream : Stream, ?forAsync) =
        let forAsync = defaultArg forAsync false
        let expectedSize = 
            if dataStream.CanSeek then dataStream.Length 
            else VirtualStream.thresholdSizeMax
        VirtualStream.Create (expectedSize, forAsync)

    interface ICloneable with
        member x.Clone () =
            let cloned =
                if isInMemory then new MemoryStream (int x.UnderlayingStream.Length) :> Stream
                else VirtualStream.createPersistentStream forAsync
                     |> fun str -> str.SetLength x.Length; str
            x.UnderlayingStream.CopyTo cloned
            cloned.Position <- 0L
            new VirtualStream (thresholdSize, memoryStatus, cloned, forAsync) :> obj

    override x.CanRead = x.UnderlayingStream.CanRead
    override x.CanWrite = x.UnderlayingStream.CanWrite
    override x.CanSeek = x.UnderlayingStream.CanSeek
    override x.Length = x.UnderlayingStream.Length
    override x.Position 
        with get () = x.UnderlayingStream.Position
        and set v = x.UnderlayingStream.Seek(v, SeekOrigin.Begin) |> ignore

    override x.Flush () = whenNotDisposed x.UnderlayingStream.Flush
    override x.Read (buffer, offset, count) = whenNotDisposed (fun () -> x.UnderlayingStream.Read (buffer, offset, count))
    override x.BeginRead (buffer, offset, count, callback, state) = whenNotDisposed (fun () -> x.UnderlayingStream.BeginRead (buffer, offset, count, callback, state))
    override x.EndRead (asyncResult) = whenNotDisposed (fun () -> x.UnderlayingStream.EndRead asyncResult)
    override x.ReadAsync (buffer, offset, count, cancellationToken) = whenNotDisposed (fun () -> x.ReadAsync (buffer, offset, count, cancellationToken))
    override x.Seek (offset, origin) = whenNotDisposed (fun () -> x.UnderlayingStream.Seek (offset, origin))
    override x.SetLength (length) = overflowToDisk length (fun () -> x.UnderlayingStream.SetLength length)
    override x.Write (buffer, offset, count) = overflowToDisk (int64 count + x.UnderlayingStream.Position) (fun () -> x.UnderlayingStream.Write (buffer, offset, count))
    override x.BeginWrite (buffer, offset, count, callback, state) = overflowToDisk (int64 count + x.UnderlayingStream.Position) (fun () -> x.UnderlayingStream.BeginWrite (buffer, offset, count, callback, state))
    override x.EndWrite (asyncResult) = whenNotDisposed (fun () -> x.UnderlayingStream.EndWrite asyncResult)
    override x.WriteAsync (buffer, offset, count, cancellationToken) = overflowToDisk (int64 count + x.UnderlayingStream.Position) (fun () -> x.UnderlayingStream.WriteAsync (buffer, offset, count, cancellationToken))
    override x.CopyToAsync (destination, bufferSize, cancellationToken) = whenNotDisposed (fun () -> x.UnderlayingStream.CopyToAsync (destination, bufferSize, cancellationToken))
    override x.Dispose (disposing) =
        try if not disposing || isDisposed then ()
            else cleanUp ()
        finally base.Dispose disposing

module StreamExtensions =
    type Stream with
        /// <summary>
        /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
        /// </summary>
        static member asVirtual (str : Stream) = 
            let vs = VirtualStream.Create str :> Stream
            str.CopyTo vs
            vs.Position <- 0L
            vs

        /// <summary>
        /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
        /// </summary>
        static member asAsyncVirtual (str : Stream) = 
            let vs = VirtualStream.Create (str, forAsync=true) :> Stream
            str.CopyTo vs
            vs.Position <- 0L
            vs

        /// <summary>
        /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
        /// </summary>
        static member asVirtualAsync (str : Stream) = async {
            let vs = VirtualStream.Create str :> Stream
            do! str.CopyToAsync vs |> Async.AwaitTask
            vs.Position <- 0L
            return vs }

        /// <summary>
        /// Creates a virtual stream that automatically overflows to disk when the length gets greater than the specified threshold.
        /// </summary>
        static member asAsyncVirtualAsync (str : Stream) = async {
            let vs = VirtualStream.Create str :> Stream
            do! str.CopyToAsync vs |> Async.AwaitTask
            vs.Position <- 0L
            return vs }
