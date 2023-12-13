import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.*;
import org.apache.kafka.clients.*;
import io.apicurio.registry.serde.*;
import io.apicurio.registry.serde.avro.*;
import io.apicurio.registry.serde.strategy.*;
import io.apicurio.registry.resolver.*;
import io.apicurio.rest.client.config.*;

import java.util.Collections;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.common.Config;
import io.strimzi.kafka.oauth.common.ConfigProperties;

public class KafkaConsumerV311 {
    public static boolean enableschemaavro=false;
    public static boolean enableintercept=false;
    public static boolean enablemtls=false;
    public static boolean enableschemamtls=false;
    public static String bootstrapServers;
    public static String sasljaas;
    public static String security;
    public static String saslmechanism;
    public static String kerberosservicename;
    public static String schemaurl;
    public static String basicauth;
    public static String basicauthuser;
    public static String basicauthpassword;
    public static String topic;
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
    public static String truststorelocation;
    public static String truststorepassword;
    public static String truststoretype;
    public static String keystorelocation;
    public static String keystorepassword;
    public static String sslkeypassword;
    public static String sslendpointidentification;
    public static String consumergroupid;
    public static String consumerclientid;
    public static KafkaConsumer<String, GenericRecord> avroConsumer;
    public static KafkaConsumer<String, String> nonavroConsumer;
    public static GenericRecord customer;

    public static void start(String[] args) {
	int count = 1;
	String propertiesfile = "./config.properties";
        if (args.length == 1) {
          propertiesfile = args[0];
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
//	    retries = prop.getProperty("retries");
	    security = prop.getProperty("security.protocol");
	    saslmechanism = prop.getProperty("sasl.mechanism");
	    topic = prop.getProperty("topic");
	    sslendpointidentification = prop.getProperty("ssl.endpoint.identification.algorithm");
    	    consumergroupid = prop.getProperty("group.id");
	    consumerclientid = prop.getProperty("client.id");
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
		if (!enableschemamtls) {
		   basicauth = prop.getProperty("basic.auth.credentials.source");
                   basicauthuser = prop.getProperty("schema.registry.basic.auth.user");
                   basicauthpassword = prop.getProperty("schema.registry.basic.auth.password");
		}
//                basicauth = prop.getProperty("basic.auth.credentials.source");
//                basicauthuser = prop.getProperty("schema.registry.basic.auth.user");
//                basicauthpassword = prop.getProperty("schema.registry.basic.auth.password");
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

        Properties properties = new Properties();
        // normal consumer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
	properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, security);
	properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, sasljaas);
	properties.setProperty(SaslConfigs.SASL_MECHANISM, saslmechanism);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumergroupid);
        properties.put("client.id", consumerclientid);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (security.contains("SSL")){
                properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorelocation);
		properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoretype);
		properties.put("ssl.endpoint.identification.algorithm", sslendpointidentification);
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
                   properties.put(ApicurioClientConfig.APICURIO_REQUEST_TRUSTSTORE_LOCATION, schematruststorelocation);
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
                   properties.put(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_LOCATION, schemakeystorelocation);
                   properties.put(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_PASSWORD, schemakeystorepassword);
                   properties.put(ApicurioClientConfig.APICURIO_REQUEST_KEYSTORE_TYPE, schemakeystoretype);
        }
        // avro part (deserializer)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());


        if (enableintercept) {
                properties.setProperty("interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
                properties.setProperty("confluent.monitoring.interceptor.security.protocol", intercept_security);
                properties.setProperty("confluent.monitoring.interceptor.sasl.mechanism", intercept_saslmechanism);
                properties.setProperty("confluent.monitoring.interceptor.sasl.jaas.config", intercept_sasljaas);
                properties.setProperty("confluent.monitoring.interceptor.bootstrap.servers", intercept_bootstrapServers);
        }
        if (enableschemaavro) {
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
                properties.setProperty(SerdeConfig.REGISTRY_URL, schemaurl);
		if (!enableschemamtls) {
                   properties.setProperty(SerdeConfig.AUTH_USERNAME, basicauthuser);
                   properties.setProperty(SerdeConfig.AUTH_PASSWORD, basicauthpassword);
		}
		avroConsumer = new KafkaConsumer <String, GenericRecord>(properties);
		avroConsumer.subscribe(Collections.singleton(topic));
        } else {
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		nonavroConsumer = new KafkaConsumer<String, String>(properties);
		nonavroConsumer.subscribe(Collections.singleton(topic));
        }
	

        while (true){
            System.out.println("Polling");
	    if (enableschemaavro) {
            	ConsumerRecords<String, GenericRecord> records = avroConsumer.poll(Duration.ofMillis(1000));
		System.out.println("Number of new Records Found: " + records.count());
            	for (ConsumerRecord<String, GenericRecord> record : records){
//                	GenericRecord customer = record.value();
                	customer = record.value();
			System.out.println("Count: " + count);
                	System.out.println(customer);
			count=count+1;  
            	}


	
            	avroConsumer.commitSync();
//		avroConsumer.close();
	    }
	    if (!enableschemaavro) {
		ConsumerRecords<String, String> records = nonavroConsumer.poll(Duration.ofMillis(3000L));
		System.out.println("Number of new Records Found: " + records.count());
		for (ConsumerRecord<String, String> record : records) {
                        System.out.println("Count: " + count + "  " + record.value());
                        count=count+1;
                }
	    }//end if
        }
    }
}
