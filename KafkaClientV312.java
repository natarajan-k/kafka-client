public class KafkaClientV312 {
    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            String option= args[0]; 
            String[] args2=new String[0];               
            if( args.length==1){
	      if(option.equalsIgnoreCase("version")){
                System.out.println("Kafka Client Version 3.12");
               }
            }

            if( args.length>1){
                args2= new String[args.length-1];
                System.arraycopy(args, 1, args2, 0, args2.length); 
	    }
            
            if(option.equalsIgnoreCase("producer")) {
                new KafkaProducerV312().start(args2);
            }
            else if(option.equalsIgnoreCase("consumer")){                
//		System.out.println("Not Ready Yet");
                new KafkaConsumerV312().start(args2);
	    }
	    else if(option.equalsIgnoreCase("custom")){
		new KafkaCustomMessageProducerV312().start(args2);
            }
	    else if(option.equalsIgnoreCase("version")){
            }
	    else {
		System.out.println("Usage For Producer: java -jar KafkaClient.jar producer <number_of_records> <config_file>");
		System.out.println("Usage For Consumer: java -jar KafkaClient.jar consumer <config_file>");
		System.out.println("To get Version: java -jar KafkaClient.jar version \n");
	    }
        }
	else {
		System.out.println("Usage For Producer: java -jar KafkaClient.jar producer <number_of_records> <config_file>");
		System.out.println("Usage For Consumer: java -jar KafkaClient.jar consumer <config_file>");
		System.out.println("To get Version: java -jar KafkaClient.jar version \n");
	}
    }
}
