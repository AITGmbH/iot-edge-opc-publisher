using System;
using System.Collections.Generic;
using System.Text;

namespace OpcPublisher.AIT
{
    /// <summary>
    /// OpcPublisherPublishState enum describes the current publish state in opc publisher
    /// </summary>
    public enum OpcPublisherPublishState
    {
        None,
        Published,
        Add,
        Remove,
        Update
    }
}
