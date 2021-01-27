package com.happiestmind.Elasticsearch.web.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.happiestmind.Elasticsearch.config.elasticSearchConfig;
import com.happiestmind.Elasticsearch.web.model.Employee;
import org.apache.commons.io.FileUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@RestController
public class ElasticSearchController {
  @Autowired private elasticSearchConfig elasticSearchConfig;

  @GetMapping("/health")
  public ClusterHealthResponse getWelcome() throws IOException {
    return elasticSearchConfig
        .client()
        .cluster()
        .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
  }

  @PostMapping("/employee")
  public void addEmployees(@RequestBody List<Employee> employees) throws IOException {
    BulkRequest bulkRequest = new BulkRequest();
    for (Employee employee : employees) {
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      String json = ow.writeValueAsString(employee);
      bulkRequest.add(
          new IndexRequest().index("company").type("employee").source(json, XContentType.JSON));
    }
    elasticSearchConfig.client().bulk(bulkRequest, RequestOptions.DEFAULT);
  }

  @GetMapping("/sorts")
  public SearchResponse getEmployee() throws IOException {
    SearchSourceBuilder builder =
        new SearchSourceBuilder()
            .from(0)
            .size(5)
            .sort("experienceInYears", SortOrder.fromString("ASC"))
            .fetchSource(false);
    return elasticSearchConfig
        .client()
        .search(new SearchRequest().indices("company").source(builder), RequestOptions.DEFAULT);
  }

  @GetMapping("/term")
  public ResponseEntity<Object> getTerm() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\term.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/sort")
  public ResponseEntity<Object> sortOnIndex() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\sort.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/range")
  public ResponseEntity<Object> findByGivenRange() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\range.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/prefix")
  public ResponseEntity<Object> findByPrefix() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\prefix.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/phrase")
  public ResponseEntity<Object> findByPhrase() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\phrase.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/multiMatch")
  public ResponseEntity<Object> findByMultiMatch() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\multiMatch.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/matchWithMultiWord")
  public ResponseEntity<Object> findByMatchWithMultiMatch() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\matchWithMultiWord.json"),
            StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/mustShouldCombination")
  public ResponseEntity<Object> findByMustAndShould() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\mustShouldCombination.json"),
            StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/buket")
  public ResponseEntity<Object> dateHistogram() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\dateHistogram.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/valueCount")
  public ResponseEntity<Object> valueCount() throws IOException {
    Request request = new Request("get", "/index4/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\valueCount.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/termAgg")
  public ResponseEntity<Object> termAggarigation() throws IOException {
    Request request = new Request("get", "/index4/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\termAggarigation.json"),
            StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/nasted")
  public ResponseEntity<Object> nastedAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\filterAggarigation.json"),
            StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/top")
  public ResponseEntity<Object> topAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\top.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/stats")
  public ResponseEntity<Object> statsAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\stats.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/sum")
  public ResponseEntity<Object> sumAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\sum.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/rangeAgg")
  public ResponseEntity<Object> rangeAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\RangeAggarigation.json"),
            StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }

  @GetMapping("/max")
  public ResponseEntity<Object> maxAggarigation() throws IOException {
    Request request = new Request("get", "/index3/_search");
    request.setJsonEntity(
        FileUtils.readFileToString(
            new File("src\\main\\resources\\maxAggarigation.json"), StandardCharsets.UTF_8.name()));
    Response response = elasticSearchConfig.lowLevelClient().performRequest(request);
    return new ResponseEntity<>(
        new JSONObject(EntityUtils.toString(response.getEntity())).toMap(), HttpStatus.OK);
  }
}
