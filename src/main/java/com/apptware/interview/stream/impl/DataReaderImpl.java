package com.apptware.interview.stream.impl;

import com.apptware.interview.stream.DataReader;
import com.apptware.interview.stream.PaginationService;
import jakarta.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
class DataReaderImpl implements DataReader {

  @Autowired private PaginationService paginationService;

  @Override
  public Stream<String> fetchLimitadData(int limit) {
    return fetchPaginatedDataAsStream().limit(limit);
  }

  @Override
  public Stream<String> fetchFullData() {
    return fetchPaginatedDataAsStream();
  }

  /**
   * This method is where the candidate should add the implementation. Logs have been added to track
   * the data fetching behavior. Do not modify any other areas of the code.
   */
  private @Nonnull Stream<String> fetchPaginatedDataAsStream() {
    log.info("Fetching paginated data as stream.");
    int pageSize = 100; // You can adjust this based on your requirements.
    int totalPages = (int) Math.ceil((double) PaginationService.FULL_DATA_SIZE / pageSize);

    return Stream.iterate(1, page -> page + 1)
            .limit(totalPages)
            .flatMap(page -> {
              List<String> pageData = paginationService.getPaginatedData(page, pageSize);
              return pageData.stream();
            })
            .peek(item -> log.info("Fetched Item: {}", item));
  }
}
