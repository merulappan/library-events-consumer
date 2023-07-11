package intg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.libraryeventsconsumer.LibraryEventsConsumerApplication;
import com.learn.kafka.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.learn.kafka.libraryeventsconsumer.entity.Book;
import com.learn.kafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learn.kafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learn.kafka.libraryeventsconsumer.jpa.LibraryEventsRepository;
import com.learn.kafka.libraryeventsconsumer.service.LibraryEventsService;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = 
{"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
 "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@ContextConfiguration(classes=LibraryEventsConsumerApplication.class)
class LibraryEventsConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer,String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry kafkaListenerEndPointRegistry;
	
	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;
	
	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;
	
    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    
    @Autowired
    ObjectMapper objectMapper;
	
	@BeforeEach
	void setup() {
		for(MessageListenerContainer listenerContainer : kafkaListenerEndPointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown() {
		libraryEventsRepository.deleteAll();
	}
	
	@Test
	void publishKafkaLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		String json = "{\r\n"
				+ "\"libraryEventId\": null,\r\n"
				+ "\"libraryEventType\": \"NEW\",\r\n"
				+ "\"book\": {\r\n"
				+ "	\"bookId\": 456,\r\n"
				+ "	\"bookName\": \"Apache Kafka\",\r\n"
				+ "	\"bookAuthor\": \"KeMk\"\r\n"
				+ "}\r\n"
				+ "}";
		
		kafkaTemplate.sendDefault(json).get();
		
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3,TimeUnit.SECONDS);
		
		verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
		
		assert libraryEvents.size() == 1;
		
		libraryEvents.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(456,libraryEvent.getBook().getBookId());
		});
	}
	
	@Test
	void publishUpdateLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
       LibraryEvent libraryEvent = objectMapper.readValue(json,LibraryEvent.class);
       libraryEvent.getBook().setLibraryEvent(libraryEvent);
       libraryEventsRepository.save(libraryEvent);
       
       Book updatedBook = Book.builder().bookId(456).bookAuthor("Keml").bookName("Kafka 2.x").build();
       libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
       libraryEvent.setBook(updatedBook);
       
       String updatedJson = objectMapper.writeValueAsString(libraryEvent);
       
       kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();
       
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3,TimeUnit.SECONDS);
		
		verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("Kafka 2.x", persistedLibraryEvent.getBook().getBookName());
	}

}
