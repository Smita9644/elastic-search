package com.happiestmind.Elasticsearch.web.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Generated;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/** Role basic info. */
@Getter
@Builder
@JsonDeserialize(builder = Employee.EmployeeBuilder.class)
@Generated
@ToString
public class Employee implements Serializable {
  /** Name. */
  private String name;
  /** Age. */
  private Long age;
  /** Experience. */
  private Long experienceInYears;

  /** Role basic info Builder inner class. */
  @JsonPOJOBuilder(withPrefix = "")
  public static class EmployeeBuilder {}
}
