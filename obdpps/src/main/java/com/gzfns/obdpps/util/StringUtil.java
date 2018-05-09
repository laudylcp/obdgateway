package com.gzfns.obdpps.util;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class StringUtil {
    private static final char[] HEX_CHAR = {'0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     *
     * byte[] to hex string
     *
     * @param bytes
     * @return
     */
    public static String bytesToHexFun1(byte[] bytes) {
        // 一个byte为8位，可用两个十六进制位标识
        char[] buf = new char[bytes.length * 2];
        int a = 0;
        int index = 0;
        for(byte b : bytes) { // 使用除与取余进行转换
            if(b < 0) {
                a = 256 + b;
            } else {
                a = b;
            }

            buf[index++] = HEX_CHAR[a / 16];
            buf[index++] = HEX_CHAR[a % 16];
        }

        return new String(buf);
    }

    /**
     *
     * byte[] to hex string
     *
     * @param bytes
     * @return
     */
    public static String bytesToHexFun2(byte[] bytes) {
        char[] buf = new char[bytes.length * 2];
        int index = 0;
        for(byte b : bytes) { // 利用位运算进行转换，可以看作方法一的变种
            buf[index++] = HEX_CHAR[b >>> 4 & 0xf];
            buf[index++] = HEX_CHAR[b & 0xf];
        }

        return new String(buf);
    }

    /**
     *
     * byte[] to hex string
     *
     * @param bytes
     * @return
     */
    public static String bytesToHexFun3(byte[] bytes) {
        StringBuilder buf = new StringBuilder(bytes.length * 2);
        for(byte b : bytes) { // 使用String的format方法进行转换
            buf.append(String.format("%02x", new Integer(b & 0xff)));
        }

        return buf.toString();
    }

    /**
     * 将16进制字符串转换为byte[]
     *
     * @param str
     * @return
     */
    public static byte[] toBytes(String str) {
        if(str == null || str.trim().equals("")) {
            return new byte[0];
        }

        byte[] bytes = new byte[str.length() / 2];
        for(int i = 0; i < str.length() / 2; i++) {
            String subStr = str.substring(i * 2, i * 2 + 2);
            bytes[i] = (byte) Integer.parseInt(subStr, 16);
        }

        return bytes;
    }

    /**
     * @Title:bytes2HexString
     * @Description:字节数组转16进制字符串
     * @param b
     *            字节数组
     * @return 16进制字符串
     * @throws
     */
    public static String bytes2HexString(byte[] b) {
        StringBuffer result = new StringBuffer();
        String hex;
        for (int i = 0; i < b.length; i++) {
            hex = Integer.toHexString(b[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            result.append(hex.toUpperCase());
        }
        return result.toString();
    }

    /**
     * @Title:hexString2Bytes
     * @Description:16进制字符串转字节数组
     * @param src
     *            16进制字符串
     * @return 字节数组
     * @throws
     */
    public static byte[] hexString2Bytes(String src) {
        int l = src.length() / 2;
        byte[] ret = new byte[l];
        for (int i = 0; i < l; i++) {
            ret[i] = (byte) Integer
                    .valueOf(src.substring(i * 2, i * 2 + 2), 16).byteValue();
        }
        return ret;
    }

    /**
     * @Title:string2HexString
     * @Description:字符串转16进制字符串
     * @param strPart
     *            字符串
     * @return 16进制字符串
     * @throws
     */
    public static String string2HexString(String strPart) {
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < strPart.length(); i++) {
            int ch = (int) strPart.charAt(i);
            String strHex = Integer.toHexString(ch);
            hexString.append(strHex);
        }
        return hexString.toString();
    }

    /**
     * @Title:hexString2String
     * @Description:16进制字符串转字符串
     * @param src
     *            16进制字符串
     * @return 字节数组
     * @throws
     */
    public static String hexString2String(String src) {
        String temp = "";
        for (int i = 0; i < src.length() / 2; i++) {
            temp = temp
                    + (char) Integer.valueOf(src.substring(i * 2, i * 2 + 2),
                    16).byteValue();
        }
        return temp;
    }

    /**
     * @Title:char2Byte
     * @Description:字符转成字节数据char-->integer-->byte
     * @param src
     * @return
     * @throws
     */
    public static Byte char2Byte(Character src) {
        return Integer.valueOf((int)src).byteValue();
    }

    /**
     * @Title:intToHexString
     * @Description:10进制数字转成16进制
     * @param a 转化数据
     * @param len 占用字节数
     * @return
     * @throws
     */
    private static String intToHexString(int a,int len){
        len<<=1;
        String hexString = Integer.toHexString(a);
        int b = len -hexString.length();
        if(b>0){
            for(int i=0;i<b;i++)  {
                hexString = "0" + hexString;
            }
        }
        return hexString;
    }

     /**
     * 判断是否是一个中文汉字
     *
     * @param c
     *            字符
     * @return true表示是中文汉字，false表示是英文字母
     * @throws UnsupportedEncodingException
     *             使用了JAVA不支持的编码格式
     */
    public static boolean isChineseChar(char c, String charsetName)
            throws UnsupportedEncodingException {

        // 如果字节数大于1，是汉字
        // 以这种方式区别英文字母和中文汉字并不是十分严谨，但在这个题目中，这样判断已经足够了
        return String.valueOf(c).getBytes(charsetName).length > 1;
    }

    /**
     * 计算当前String字符串所占的总Byte长度
     * @param args
     *              要截取的字符串
     * @return
     *              返回值int型，字符串所占的字节长度，如果args为空或者“”则返回0
     * @throws UnsupportedEncodingException
     */
    public static int getStringByteLenths(String args, String charsetName) throws UnsupportedEncodingException{
        return args!=null&&args!=""? args.getBytes(charsetName).length:0;
    }

    /**
     * 获取与字符串每一个char对应的字节长度数组
     * @param  args
     *              要计算的目标字符串
     * @return int[]
     *              数组类型，返回与字符串每一个char对应的字节长度数组
     * @throws UnsupportedEncodingException
     */
    public static int[] getByteLenArrays(String args, String charsetName) throws UnsupportedEncodingException{
        char[] strlen=args.toCharArray();
        int[] charlen=new int[strlen.length];
        for (int i = 0; i < strlen.length; i++) {
            charlen[i]=String.valueOf(strlen[i]).getBytes(charsetName).length;
        }
        return charlen;
    }

    /**
     * 按字节截取字符串 ，指定截取起始字节位置与截取字节长度
     *
     * @param orignal
     *              要截取的字符串
     * @param count
     *              截取Byte长度；
     * @return
     *              截取后的字符串
     * @throws UnsupportedEncodingException
     *              使用了JAVA不支持的编码格式
     */
    public static String substringByte(String orignal,int start, int count, String charsetName){

        //如果目标字符串为空，则直接返回，不进入截取逻辑；
        if(orignal==null || "".equals(orignal))return orignal;

        //截取Byte长度必须>0
        if (count <= 0) return orignal;

        //截取的起始字节数必须比
        if(start < 0) start=0;

        //目标char Pull buff缓存区间；
        StringBuffer buff = new StringBuffer();

        try {

            //截取字节起始字节位置大于目标String的Byte的length则返回空值
            if (start >= getStringByteLenths(orignal, charsetName)) return null;

            // int[] arrlen=getByteLenArrays(orignal);
            int len=0;

            char c;

            //遍历String的每一个Char字符，计算当前总长度
            //如果到当前Char的的字节长度大于要截取的字符总长度，则跳出循环返回截取的字符串。
            for (int i = 0; i < orignal.toCharArray().length; i++) {

                c = orignal.charAt(i);

                //当起始位置为0时候
                if(start==0){

                    len+=String.valueOf(c).getBytes(charsetName).length;
                    if(len<=count) buff.append(c);
                    else break;

                }else{

                    //截取字符串从非0位置开始
                    len+=String.valueOf(c).getBytes(charsetName).length;
                    if(len>=start&&len<=start+count){
                        buff.append(c);
                    }
                    if(len>start+count) break;

                }
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //返回最终截取的字符结果;
        //创建String对象，传入目标char Buff对象
        return new String(buff);
    }

    /**
     * 截取指定长度字符串
     * @param orignal
     *              要截取的目标字符串
     * @param count
     *              指定截取长度
     * @return
     *              返回截取后的字符串
     */
    public static String substringByte(String orignal, int count, String charsetName){
        return substringByte(orignal,0,count, charsetName);
    }

//        private String substr(String originString, String charsetName, int byteLen)
//            throws UnsupportedEncodingException {
//        if (originString == null || originString.isEmpty() || byteLen <= 0) {
//            return "";
//        }
//        char[] chars = originString.toCharArray();
//        int length = 0, index = chars.length;
//        for (int i = 0; i < chars.length; ++i) {
//            final int len = String.valueOf(chars[i]).getBytes(charsetName).length + length;
//            if (len <= byteLen) {
//                length = len;
//            } else {
//                index = i;
//                break;
//            }
//        }
//        return String.valueOf(chars, 0, index);
//    }

    public static void main(String[] args) throws Exception {
        byte[] bytes = "测试".getBytes("ASCII");
        System.out.println("字节数组为：" + Arrays.toString(bytes));
        System.out.println("方法一：" + bytesToHexFun1(bytes));
        System.out.println("方法二：" + bytesToHexFun2(bytes));
        System.out.println("方法三：" + bytesToHexFun3(bytes));

        System.out.println("==================================");

        String str = "e6b58be8af95";
        System.out.println("转换后的字节数组：" + Arrays.toString(toBytes(str)));
        System.out.println(new String(toBytes(str), "ASCII"));
    }

}
