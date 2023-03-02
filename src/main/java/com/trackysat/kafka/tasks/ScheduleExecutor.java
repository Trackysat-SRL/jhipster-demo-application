package com.trackysat.kafka.tasks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.service.CassandraConsumerService;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class ScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(ScheduleExecutor.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    private static final String VMSON_EVENT =
        " {\"vmson\":{\"con\":[{\"ets\":{\"uid\":\"DEVICE_TRACKING_TIME\",\"tst\":\"2023-03-02T14:22:27.000Z\"},\"sat\":{\"snr\":21,\"lon\":10.215015,\"spe\":88,\"fix\":\"FIX2D\",\"dir\":118,\"alt\":113,\"lat\":45.4648166,\"sig\":\"6\"},\"sen\":[{\"val\":\"1\",\"sid\":\"1\",\"mis\":\"bool\",\"iid\":\"KeyStatus\",\"typ\":\"POWER\",\"src\":\"DEVICE\"},{\"val\":\"9863\",\"sid\":\"1\",\"mis\":\"mV\",\"iid\":\"InternalBatteryLevel\",\"typ\":\"ELECTRICPOTENTIAL\",\"src\":\"DEVICE\"},{\"val\":\"28327\",\"sid\":\"2\",\"mis\":\"mV\",\"iid\":\"ExternalBatteryLevel\",\"typ\":\"ELECTRICPOTENTIAL\",\"src\":\"DEVICE\"},{\"val\":\"98300690\",\"sid\":\"1\",\"mis\":\"m\",\"iid\":\"TotalVehicleDistance\",\"typ\":\"DISTANCE\",\"src\":\"DEVICE\"},{\"val\":\"1\",\"sid\":\"1\",\"mis\":\"bool\",\"iid\":\"InputStatus\",\"typ\":\"BOOLEAN\",\"src\":\"DEVICE\"},{\"val\":\"0\",\"sid\":\"2\",\"mis\":\"bool\",\"iid\":\"InputStatus\",\"typ\":\"BOOLEAN\",\"src\":\"DEVICE\"},{\"val\":\"0\",\"sid\":\"1\",\"mis\":\"bool\",\"iid\":\"OutputStatus\",\"typ\":\"BOOLEAN\",\"src\":\"DEVICE\"},{\"val\":\"33300.09\",\"sid\":\"1\",\"mis\":\"L\",\"iid\":\"TotalFuel\",\"typ\":\"VOLUME\",\"src\":\"CAN\"},{\"val\":\"99092.59\",\"sid\":\"1\",\"mis\":\"km\",\"iid\":\"TotalVehicleDistance\",\"typ\":\"DISTANCE\",\"src\":\"CAN\"},{\"val\":\"34.80\",\"sid\":\"1\",\"mis\":\"perc\",\"iid\":\"FuelLevelPercentage\",\"typ\":\"PERCENTAGE\",\"src\":\"CAN\"},{\"val\":\"0\",\"sid\":\"1\",\"iid\":\"BrakeStatus\",\"typ\":\"STATUS\",\"src\":\"CAN\"},{\"val\":\"1\",\"sid\":\"1\",\"iid\":\"CruiseControlStatus\",\"typ\":\"STATUS\",\"src\":\"CAN\"},{\"val\":\"2074\",\"sid\":\"1\",\"mis\":\"h\",\"iid\":\"TimeEngineLife\",\"typ\":\"TIME\",\"src\":\"CAN\"},{\"val\":\"86\",\"sid\":\"1\",\"mis\":\"C\",\"iid\":\"EngineTemperature\",\"typ\":\"TEMPERATURE\",\"src\":\"CAN\"},{\"val\":\"0\",\"sid\":\"1\",\"iid\":\"PTOStateStatus\",\"typ\":\"STATUS\",\"src\":\"CAN\"},{\"val\":\"-99095\",\"sid\":\"1\",\"mis\":\"km\",\"iid\":\"ServiceDistance\",\"typ\":\"DISTANCE\",\"src\":\"CAN\"},{\"val\":\"17\",\"sid\":\"1\",\"mis\":\"lh\",\"iid\":\"FuelRate\",\"typ\":\"VOLUME\",\"src\":\"CAN\"},{\"val\":\"5\",\"sid\":\"1\",\"mis\":\"kml\",\"iid\":\"IstantaneousFuelEconomy\",\"typ\":\"VOLUME\",\"src\":\"CAN\"}]}],\"uid\":\"9A00A09D578E5717C1DC6AE0AFBD2F2A\",\"des\":{\"uid\":\"DRIVER_TELTONIKA_FMB640_42-01-0A-84-00-03\",\"cla\":\"MODULE\",\"ver\":\"1.2.12.RELEASE\",\"typ\":\"DRIVER\"},\"ets\":{\"uid\":\"COMMAND_COMMUNICATION\",\"tst\":\"2023-03-02T14:22:29.399Z\"},\"ori\":{\"uid\":\"352625695512226\",\"cla\":\"TELTONIKA_FMB640\",\"typ\":\"DEVICE\"}}}";
    private static final String VMSON_ERROR = "<test>M</test>";
    private static final int EVENTS_PER_SECOND = 30;

    @Value(value = "${kafka.producer.enabled}")
    private boolean enabled;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedRate = 1000)
    public void produceJson() throws JsonProcessingException {
        if (enabled) {
            for (int i = 0; i < EVENTS_PER_SECOND; i++) {
                int random_int = (int) Math.floor(Math.random() * (10 - 1 + 1) + 1);
                String message = random_int < 3 ? VMSON_EVENT : VMSON_ERROR;
                kafkaTemplate
                    .send(CassandraConsumerService.TRACKYSAT_TOPIC, message)
                    .addCallback(
                        new ListenableFutureCallback<SendResult<String, String>>() {
                            @Override
                            public void onSuccess(SendResult<String, String> result) {
                                log.info("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                            }

                            @Override
                            public void onFailure(Throwable ex) {
                                log.error("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                            }
                        }
                    );
            }
        }
    }
}
