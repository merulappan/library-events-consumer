package com.learn.kafka.libraryeventsconsumer.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {
	
	@Id
	@GeneratedValue
	private Integer libraryEventId;
	@OneToOne(mappedBy = "libraryEvent",cascade = {CascadeType.ALL})
	@ToString.Exclude
	private Book book;
	@Enumerated(EnumType.STRING)
	private LibraryEventType libraryEventType;

}
