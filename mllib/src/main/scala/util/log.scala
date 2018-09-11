package demy.mllib.util

object log {
    def msg(message:Any) {
        val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        val now = new java.util.Date();
        val strDate = sdfDate.format(now);
        println(strDate+"-->"+message);
    }
}
