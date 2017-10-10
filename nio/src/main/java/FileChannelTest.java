public class FileChannelTest {

//    public static void main(String[] args) throws IOException {
//        RandomAccessFile file = null;
//        RandomAccessFile wfile = null;
//        try {
//            file = new RandomAccessFile("./test", "r");
//            wfile = new RandomAccessFile("./test1", "rw");
//            FileChannel channel = file.getChannel();
//            FileChannel wChannel = wfile.getChannel();
//            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
//            int count = 0;
//            while((count = channel.read(byteBuffer)) != -1) {
//                System.out.println("已经读取" + count + "个字节");
//                byteBuffer.flip(); //把读模式转成写模式，反之亦然
//                wChannel.truncate(512);
//                while(byteBuffer.hasRemaining()) {
////                    long position = wChannel.position();
////                    if(position != 0) {
////                        channel.position(position +123);
////
////                    }
//
//                    wChannel.write(byteBuffer);
//                }
//                //System.out.println();
//                byteBuffer.clear();
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } finally {
//            if (file != null) {
//                file.close();
//            }
//        }
//
//    }


}
