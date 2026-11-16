using System.Runtime.CompilerServices;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>
/// For the members of the Azure clients that the Lucene-backed clients do not implement. A member
/// that is not overridden runs the base client's code, which has no connection and fails with a
/// <see cref="NullReferenceException"/>; this says what is wrong instead.
/// </summary>
internal static class NotSupported
{
    public static T Throw<T>([CallerMemberName] string memberName = "")
        => throw new NotSupportedException($"'{memberName}' is not supported by the Lucene-backed client.");

    public static T ThrowWithMessage<T>(string message)
        => throw new NotSupportedException(message);
}
