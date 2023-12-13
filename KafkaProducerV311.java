
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.*;
import org.apache.kafka.clients.*;

import io.apicurio.registry.serde.*;
import io.apicurio.registry.serde.avro.*;
import io.apicurio.registry.serde.strategy.*;
import io.apicurio.registry.resolver.*;
import io.apicurio.rest.client.config.*;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.Base64;

import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.common.Config;
import io.strimzi.kafka.oauth.common.ConfigProperties;


public class KafkaProducerV311 {
    public static boolean enableschemaavro=false;
    public static String autoregisterschemas="false";
    public static boolean enableintercept=false;
    public static boolean enablemtls=false;
    public static boolean enableschemamtls=false;
    public static String bootstrapServers;
    public static String sasljaas;
    public static String retries;
    public static String security;
    public static String saslmechanism;
    public static String kerberosservicename;
    public static String schemaurl;
    public static String basicauth;
    public static String basicauthuser;
    public static String basicauthpassword;
    public static String topic;
    public static String truststorelocation;
    public static String truststorepassword;
    public static String truststoretype;
    public static String keystorelocation;
    public static String keystorepassword;
    public static String sslkeypassword;
    public static String schematruststorelocation;
    public static String schematruststorepassword;
    public static String schematruststoretype;
    public static String schemakeystorelocation;
    public static String schemakeystorepassword;
    public static String schemakeystoretype;
    public static String intercept_bootstrapServers;
    public static String intercept_sasljaas;
    public static String intercept_security;
    public static String intercept_saslmechanism;
    public static String sslendpointidentification; 
    private static String file1Name = "file1.txt";
    private static String file2Name = "file2.txt";
    private static String file3Name = "file3.txt";
    private static int numofrecords = 0;
    public static KafkaProducer<String, String> nonavroproducer;
    public static KafkaProducer<String, GenericRecord> avroproducer;
    public static final String REGISTRY_CONVERTER_DESERIALIZER_PARAM = "apicurio.registry.converter.deserializer";
    public static GenericRecord customer;
    public static void start(String[] args) {

//	Parse Schema File
	Schema.Parser schemaDefinitionParser = new Schema.Parser();
	try {
		Schema schema = schemaDefinitionParser.parse(new File("./customer.avsc"));
		customer = new GenericData.Record(schema);
	}
  		catch (IOException e) {
  	}
	List<String> file1lines = null;
	List<String> file2lines = null;
	List<String> file3lines = null;
	int loadsize = 1;
	String propertiesfile = "./config.properties";
      	if (args.length == 2) {
	  propertiesfile = args[1];
	}
       	try (InputStream input = new FileInputStream(propertiesfile)) {
            Properties prop = new Properties();
            prop.load(input);
	    enableschemaavro = Boolean.parseBoolean(prop.getProperty("enableschemaavro"));
	    enableintercept = Boolean.parseBoolean(prop.getProperty("enableintercept"));
	    enablemtls = Boolean.parseBoolean(prop.getProperty("enablemtls"));
	    enableschemamtls = Boolean.parseBoolean(prop.getProperty("enableschemamtls"));
    	    bootstrapServers = prop.getProperty("bootstrap.servers");
	    sasljaas = prop.getProperty("sasl.jaas.config");
	    retries = prop.getProperty("retries");
	    security = prop.getProperty("security.protocol");
	    saslmechanism = prop.getProperty("sasl.mechanism");
	    sslendpointidentification = prop.getProperty("ssl.endpoint.identification.algorithm");
	    topic = prop.getProperty("topic");
	    if (security.contains("SSL")){
		truststorelocation = prop.getProperty("ssl.truststore.location");
                truststoretype = prop.getProperty("ssl.truststore.type");
                if (truststoretype.contains("PKCS12") || truststoretype.contains("JKS")){
                        truststorepassword = prop.getProperty("ssl.truststore.password");
                }
	    }
	    if (enablemtls) {
		keystorelocation = prop.getProperty("ssl.keystore.location");
		keystorepassword = prop.getProperty("ssl.keystore.password");
		sslkeypassword = prop.getProperty("ssl.key.password");	
	    }
	    if (saslmechanism.equals("GSSAPI")) {
		kerberosservicename = prop.getProperty("sasl.kerberos.service.name");
	    }
	    if (enableschemaavro) {
		schemaurl = prop.getProperty("apicurio.registry.url");
		autoregisterschemas = prop.getProperty("auto.register.schemas");
		if (!enableschemamtls) {
		   basicauth = prop.getProperty("basic.auth.credentials.source");
		   basicauthuser = prop.getProperty("schema.registry.basic.auth.user");
		   basicauthpassword = prop.getProperty("schema.registry.basic.auth.password");
		}
	    }
            if ((enableschemaavro) && (schemaurl.contains("https"))) {
                schematruststorelocation = prop.getProperty("schema.registry.ssl.truststore.location");
                schematruststorepassword = prop.getProperty("schema.registry.ssl.truststore.password");
		schematruststoretype = prop.getProperty("schema.registry.ssl.truststore.type");
            }
            if (enableschemamtls) {
                schemakeystorelocation = prop.getProperty("schema.registry.ssl.keystore.location");
                schemakeystorepassword = prop.getProperty("schema.registry.ssl.keystore.password");
                schemakeystoretype = prop.getProperty("schema.registry.ssl.keystore.type");
            }

	    if (enableintercept) {
	    	intercept_bootstrapServers = prop.getProperty("intercept_bootstrapServers");
            	intercept_sasljaas = prop.getProperty("intercept_sasljaas");
            	intercept_security = prop.getProperty("intercept_security");
            	intercept_saslmechanism = prop.getProperty("intercept_saslmechanism");
	    }


       	} catch (IOException ex) {
            ex.printStackTrace();
       	}

	if (args.length < 1) {
		System.out.println("ERROR: Number_of_Records parameter required");
		System.exit(0);
	} else {
		loadsize = Integer.parseInt(args[0]);
	}

        Properties properties = new Properties();
        // normal producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty("retries", retries);
	properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, security);
	properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, sasljaas);
	properties.setProperty(SaslConfigs.SASL_MECHANISM, saslmechanism);

        if (security.contains("SSL")){
        	properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorelocation);
		properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoretype);
		properties.setProperty("ssl.endpoint.identification.algorithm", sslendpointidentification);
                if (truststoretype.contains("PKCS12") || truststoretype.contains("JKS")){
                        properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorepassword);
                }
	}
        if (enablemtls) {
		properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorelocation);
		properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorepassword);
		properties.setProperty("ssl.key.password", sslkeypassword);
        }
	if ((enableschemaavro) && (schemaurl.contains("https"))) {
		     properties.putIfAbsent(ApicurioClientConfig.APICURIO_REQUEST_TRUSTSTORE_LOCATION, schematruststorelocation);
		     properties.put(ApicurioClientConfig.APICURIO_REQUEST_TRUSTSTORE_PASSWORD, schematruststorepassword);
		     properties.put(ApicurioClientConfig.APICURIO_REQUEST_TRUSTSTORE_TYPE, schematruststoretype);
	}
        if (saslmechanism.equals("GSSAPI")) {
		properties.setProperty("sasl.kerberos.service.name", kerberosservicename);        
        }
        if (saslmechanism.equals("OAUTHBEARER")) {
 		properties.setProperty("sasl.login.callback.handler.class","io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
	}
        if (enableschemamtls) {
                     properties.putIfAbsent(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_LOCATION, schemakeystorelocation);
                     properties.put(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_PASSWORD, schemakeystorepassword);
                     properties.put(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_TYPE, schemakeystoretype);
        }
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        if (enableintercept) {
                properties.setProperty("interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
                properties.setProperty("confluent.monitoring.interceptor.security.protocol", intercept_security);
                properties.setProperty("confluent.monitoring.interceptor.sasl.mechanism", intercept_saslmechanism);
                properties.setProperty("confluent.monitoring.interceptor.sasl.jaas.config", intercept_sasljaas);
                properties.setProperty("confluent.monitoring.interceptor.bootstrap.servers", intercept_bootstrapServers);
        }

	if (enableschemaavro) {

		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        	properties.setProperty(SerdeConfig.REGISTRY_URL, schemaurl);
		if (!enableschemamtls) {
		   properties.setProperty(SerdeConfig.AUTH_USERNAME, basicauthuser);
		   properties.setProperty(SerdeConfig.AUTH_PASSWORD, basicauthpassword);
		}
		properties.setProperty(SerdeConfig.AUTO_REGISTER_ARTIFACT, autoregisterschemas);
		avroproducer = new KafkaProducer<>(properties);
	} else {
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		nonavroproducer = new KafkaProducer<String, String>(properties);
	}

    	try {
        	file1lines = Files.readAllLines(Paths.get(file1Name),
                StandardCharsets.UTF_8);
    	} catch (IOException e) {
        	System.out.println("File can't be opened.");
    	}
        try {
                file2lines = Files.readAllLines(Paths.get(file2Name),
                StandardCharsets.UTF_8);
        } catch (IOException e) {
                System.out.println("File can't be opened.");
        }

        try {
                file3lines = Files.readAllLines(Paths.get(file3Name),
                StandardCharsets.UTF_8);
        } catch (IOException e) {
                System.out.println("File can't be opened.");
        }


        for (int i=1; i<=loadsize; i++) { 
        int random1WordIndex = 1 + (int) (Math.random() * ((file1lines.size() - 1)));
        int random2WordIndex = 1 + (int) (Math.random() * ((file2lines.size() - 1)));
	int random3WordIndex = 1 + (int) (Math.random() * ((file3lines.size() - 1)));
	if (enableschemaavro) {
		customer.put("age", ((int)(Math.random() * ((80 - 1) + 1)) + 1));
		customer.put("last_name", file2lines.get(random2WordIndex));
		customer.put("first_name", file1lines.get(random1WordIndex));
		customer.put("country", file3lines.get(random3WordIndex));
//		customer.put("company", "IBM-MY");
		customer.put("height", ((float)(Math.random() * ((170 - 60) + 1)) + 60));
		customer.put("weight", ((float)(Math.random() * ((120 - 10) + 1)) + 10));
		customer.put("automated_email", true);
		String customerdata = customer.toString();
        	avroproducer.send(new ProducerRecord<String, GenericRecord>(topic,customer), new Callback() {
            	@Override
            	public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
			numofrecords=numofrecords+1;
                    	System.out.printf("Count: [%d], Topic: [%s], Partition: [%d], Offset: [%d], Message: [%s]%n", numofrecords, metadata.topic(), metadata.partition(), metadata.offset(), customerdata);
                } else {
                    	exception.printStackTrace();
			System.out.println("dssssssss");
                }
            	}
        });


	} // End the enableschemaavro if
	if (!enableschemaavro) {
		String message = file1lines.get(random1WordIndex) + " " + file2lines.get(random2WordIndex) + " " + file3lines.get(random3WordIndex);
		nonavroproducer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
              exception.printStackTrace();
            } else {
	      numofrecords=numofrecords+1;
  	      System.out.printf("Count: [%d], Topic: [%s], Partition: [%d], Offset: [%d], Message: [%s]%n", numofrecords, metadata.topic(), metadata.partition(), metadata.offset(), message);
            }
          }
      });
	} // End if NOT enableschemaavro

	}//end for loop
	if (enableschemaavro) {
        	avroproducer.flush();
        	avroproducer.close();
	}
	if (!enableschemaavro) {
		nonavroproducer.flush();
		nonavroproducer.close();
	}
    }
}
