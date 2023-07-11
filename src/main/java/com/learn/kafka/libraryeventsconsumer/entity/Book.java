package com.learn.kafka.libraryeventsconsumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
public class Book {
	@Id
	private Integer bookId;
	@NotBlank
	private String bookName;
	@NotBlank
	private String bookAuthor;
	
	@OneToOne
	@JoinColumn(name="libraryEventId")
	private LibraryEvent libraryEvent;

}
