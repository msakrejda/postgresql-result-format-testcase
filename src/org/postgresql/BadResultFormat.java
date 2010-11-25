package org.postgresql;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class BadResultFormat {

    public static final int FORMAT_TEXT = 0;
    public static final int FORMAT_BINARY = 1;
    public static final int OID_UNSPECIFIED = 0;
    
    public static final char DESCRIBE_STATEMENT = 'S';
    public static final char DESCRIBE_PORTAL = 'P';
    
    public static final char MESSAGE_TYPE_RFQ = 'Z';

    public static void main(String[] args) throws Exception {
        BadResultFormat test = new BadResultFormat();
        test.run();
    }

    public void run() throws Exception {
        String host = "localhost";
        int port = 5432;
        String user = "test";

        Socket s = new Socket(host, port);
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        DataInputStream in = new DataInputStream(s.getInputStream());
        final Map<String,String> params = new HashMap<String, String>();
        params.put("user", user);

        new FEMessage('\0') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                output.writeInt(196608 /* version */);
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    System.out.println(entry.getKey() + ":" + entry.getValue());
                    writeCString(output, entry.getKey());
                    writeCString(output, entry.getValue());
                }
                output.writeByte(0);                
            }
        }.send(out);
        
        // We should expect AuthenticationOk, 0 or more ParameterStatus, BackendKeyData,
        // ReadyForQuery, but we can just wait for RFQ 

        char responseType = '\0';
        while (responseType != 'Z') {
            responseType = readMessage(in);
        }

        // send Parse for S_1 : SELECT $1::int (typeOid 0)
        new FEMessage('P') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                writeCString(output, "S_1");
                writeCString(output, "SELECT $1::int");
                int[] paramTypeOids = new int[] { OID_UNSPECIFIED };
                output.writeShort(paramTypeOids.length);
                for (int i = 0; i < paramTypeOids.length; i++) {
                    output.writeInt(paramTypeOids[i]);
                }                
            }
        }.send(out);
        // send Describe for S_1
        new FEMessage('D') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                output.writeByte(DESCRIBE_STATEMENT);
                writeCString(output, "S_1");
            }
        }.send(out);
        // send Bind for S_1, anonymous portal, paramFormats: text, paramvalues: '2', result formats: binary
        new FEMessage('B') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                writeCString(output, "" /* unnamed portal */);
                writeCString(output, "S_1");
                int[] paramFormats = new int[] { FORMAT_TEXT };
                output.writeShort(paramFormats.length);
                for (int i = 0; i < paramFormats.length; i++) {
                    output.writeShort(paramFormats[i]);
                }
                // we're cheating here since we know we send the values as text
                String[] paramValues =  new String[] { "2" };
                output.writeShort(paramValues.length);
                for (int i = 0; i < paramValues.length; i++) {
                    output.writeInt(paramValues[i].length());
                    output.write(paramValues[i].getBytes(Charset.forName("UTF-8")));
                }
                int[] resultFormats = new int[] { FORMAT_BINARY };
                output.writeShort(resultFormats.length);
                for (int i = 0; i < resultFormats.length; i++) {
                    output.writeShort(resultFormats[i]);
                }
            }            
        }.send(out);

        // send Execute for all rows to anonymous portal
        new FEMessage('E') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                writeCString(output, "");
                output.writeInt(0);
            }
        }.send(out);
        
        // send Sync
        new FEMessage('S') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                /* do nothing */
            }
        }.send(out);

        // Expect ParseComplete, ParameterDescription, RowDescription, BindCompletion,
        // DataRow, CommandComplete, RFQ; again, we just wait for RFQ.
        
        responseType = '\0';
        while (responseType != 'Z') {
            responseType = readMessage(in);
        }
        new FEMessage('X') {
            @Override
            protected void writePayload(DataOutputStream output) throws Exception {
                /* do nothing */
            }
            
        }.send(out);
        s.close();
    }

    private String format(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int processed = 0;
        for (int i = 0; i < bytes.length; i++) {
            processed++;
            String hexStr = Integer.toHexString(bytes[i] & 0xFF);
            if (hexStr.length() == 1) {
                sb.append('0');
            }
            sb.append(hexStr);
            sb.append(' ');
            if (processed % 16 == 0) {
                sb.append("\n   ");
            } else if (processed % 8 == 0) {
                sb.append(' ');
            }
        }
        return sb.toString().trim();
    }

    private void writeCString(DataOutputStream out, String str) throws Exception {
        byte[] bytes = str.getBytes(Charset.forName("UTF-8"));
        out.write(bytes);
        out.writeByte(0);
    }
    
    private char readMessage(DataInputStream in) throws Exception {
        char type = (char) in.readByte();
        int len = in.readInt();
        byte[] payload = new byte[len - 4];
        int read = 0;
        while (read < payload.length) {
            read = in.read(payload, read, payload.length - read);
        }
        System.out.println("<" + type + ":" + format(payload));        
        return (char) type;
    }
    
    private static abstract class FEMessage {
        private char msgType;
        protected FEMessage(char msgType) {
            this.msgType = msgType;
        }
        protected abstract void writePayload(DataOutputStream output) throws Exception;
        public void send(DataOutputStream out) throws Exception {
            if (msgType != '\0') {
                out.writeByte(msgType);
            }
            ByteArrayOutputStream rawResult = new ByteArrayOutputStream();
            DataOutputStream result = new DataOutputStream(rawResult);
            writePayload(result);
            byte[] resultBytes = rawResult.toByteArray();
            out.writeInt(resultBytes.length + 4);
            out.write(resultBytes);
        }
    }
}