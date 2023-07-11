package com.learn.kafka.libraryeventsconsumer.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learn.kafka.libraryeventsconsumer.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	LibraryEventsRepository libraryEventsRepository;
	
	public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent {}" , libraryEvent);
		
		switch(libraryEvent.getLibraryEventType()) {
		case NEW:
					save(libraryEvent);
					break;
		case UPDATE:
					validate(libraryEvent);
					save(libraryEvent);
					break;
		default:
				log.info("Invalid Library Event Type");
		}
		
		
	}
	
	private void validate(LibraryEvent libraryEvent) {
		if(libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}
		
		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
		if(! libraryEventOptional.isPresent()) {
			throw new IllegalArgumentException("Not a valid library event");
		}
		log.info("Validation is successful for the library Event {}", libraryEventOptional.get());
	}
	
	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("Succesfully pesisted LibraryEvent");
	}

}
