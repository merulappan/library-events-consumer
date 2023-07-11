package com.learn.kafka.libraryeventsconsumer.jpa;

import org.springframework.data.repository.CrudRepository;

import com.learn.kafka.libraryeventsconsumer.entity.LibraryEvent;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent,Integer>{

}
