package guru.springframework.sfgjms.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import guru.springframework.sfgjms.config.JmsConfig;
import guru.springframework.sfgjms.model.HelloWorldMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class HelloSender {

	private final JmsTemplate jmsTemplate;
	private final ObjectMapper objectMapper;

	@Scheduled(fixedRate = 2000)
	public void sendMessage() {
		HelloWorldMessage message = HelloWorldMessage
				.builder()
				.id(UUID.randomUUID())
				.message("Hello World")
				.build();

		jmsTemplate.convertAndSend(JmsConfig.MY_QUEUE, message);

	}

	@Scheduled(fixedRate = 2000)
	public void sendAndReceiveMessage() throws JMSException {
		HelloWorldMessage message = HelloWorldMessage
				.builder()
				.id(UUID.randomUUID())
				.message("Hello")
				.build();

		Message receivedMessage = jmsTemplate.sendAndReceive(JmsConfig.MY_SEND_REC_QUEUE, session -> {
			Message helloMessage;
			try {
				helloMessage = session.createTextMessage(objectMapper.writeValueAsString(message));
				helloMessage.setStringProperty("_type", "guru.springframework.sfgjms.model.HelloWorldMessage");
				log.info("Sending hello");
				return helloMessage;
			} catch (JsonProcessingException e) {
				log.error(e.getMessage(), e);
				throw new JMSException("boom");
			}
		});
		log.info(receivedMessage.getBody(String.class));
	}
}
