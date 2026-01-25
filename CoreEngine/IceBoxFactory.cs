namespace IceForRocks.Core;

public interface IIceBoxFactory
{
    IceBox<T> Create<T>(string dbPath)
        where T : unmanaged;
}

public class IceBoxFactory : IIceBoxFactory
{
    public IceBox<T> Create<T>(string dbPath)
        where T : unmanaged
    {
        return new IceBox<T>(dbPath);
    }
}
