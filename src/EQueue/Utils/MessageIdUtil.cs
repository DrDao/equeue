using System;
using System.Linq;
using System.Net;
using ECommon.Extensions;
using ECommon.Utilities;
using EQueue.Broker;

namespace EQueue.Utils
{
    public class MessageIdUtil
    {
        private static byte[] _ipBytes;
        private static byte[] _portBytes;

        public static string CreateMessageId(long messagePosition)
        {
            if (_ipBytes == null)
            {
                _ipBytes = BrokerController.Instance.Setting.BrokerInfo.ProducerAddress.ToEndPoint().Address.GetAddressBytes();
            }
            if (_portBytes == null)
            {
                _portBytes = BitConverter.GetBytes(BrokerController.Instance.Setting.BrokerInfo.ProducerAddress.ToEndPoint().Port);
            }
            var positionBytes = BitConverter.GetBytes(messagePosition);
            var messageIdBytes = ByteUtil.Combine(_ipBytes, _portBytes, positionBytes);

            return ObjectId.ToHexString(messageIdBytes);
        }
        public static MessageIdInfo ParseMessageId(string messageId)
        {
            var messageIdBytes = ObjectId.ParseHexString(messageId);
            var ipBytes = new byte[4];
            var portBytes = new byte[4];
            var messagePositionBytes = new byte[8];

            Buffer.BlockCopy(messageIdBytes, 0, ipBytes, 0, 4);
            Buffer.BlockCopy(messageIdBytes, 4, portBytes, 0, 4);
            Buffer.BlockCopy(messageIdBytes, 8, messagePositionBytes, 0, 8);


            IPAddress parseAddress = null;
            var messagePosition = BitConverter.ToInt64(messagePositionBytes, 0);
            var messageInfo = new MessageIdInfo();
            try
            {
                parseAddress = new IPAddress(BitConverter.ToUInt32(ipBytes, 0));
            }
            catch
            {
                new Exception("ParseAddress error.");
            }
            var port = BitConverter.ToInt32(portBytes, 0);
            messageInfo.IP = parseAddress;
            messageInfo.Port = port;
            messageInfo.MessagePosition = messagePosition;
            return messageInfo;
        }
    }
    public struct MessageIdInfo
    {
        public IPAddress IP;
        public int Port;
        public long MessagePosition;
    }
}
