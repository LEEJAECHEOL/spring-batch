package com.batch.project.job.readerJob;

import com.batch.project.entity.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

/**
 * processor는 필수가 아
 * reader에서 읽은 데이터에 대해 크게 변경 로직이 없다면
 * processor를 제외하고 writer만 구현하면 됨.
 */

@Slf4j
@RequiredArgsConstructor
@Configuration
public class JdbcCursorItemReaderJobConfig {
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final DataSource dataSource;

  private static final int chunkSize = 10;

  @Bean
  public Job jdbcCursorItemReaderJob() {
    return jobBuilderFactory.get("jdbcCursorItemReaderJob")
      .start(jdbcCursorItemReaderStep())
      .build();
  }

  @Bean
  public Step jdbcCursorItemReaderStep() {
    return stepBuilderFactory.get("jdbcCursorItemReaderStep")
      // 첫번째 Pay는 Reader에서 반환할 타입, 두번째 Pay는 Writer에 파라미터로 넘어올 타입
      // chunkSize로 인자값을 넣은 경우는 Reader & Writer가 묶일 Chunk 트랜잭션 범위
      .<Pay, Pay>chunk(chunkSize)
      .reader(jdbcCursorItemReader())
      .writer(jdbcCursorItemWriter())
      .build();
  }

  @Bean
  public JdbcCursorItemReader<Pay> jdbcCursorItemReader() {
    return new JdbcCursorItemReaderBuilder<Pay>()
      // Database에서 한번에 가져올 데이터 양
      .fetchSize(chunkSize)
      // Database에 접근하기 위해 사용할 Datasource 객체를 할당
      .dataSource(dataSource)
      // 쿼리 결과를 Java 인스턴스로 매핑하기 위한 Mapper
      // 커스텀하게 생성해서 사용할 수 도 있지만, 이렇게 될 경우, 매번 Mapper 클래스를 생성해야 됨.
      // 보편적으로는 Spring에서 공식적으로 지원하는 BeanPropertyRowMapper.class를 많이 사용
      .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
      // Reader로 사용할 쿼리문을 사용하면 됨.
      .sql("SELECT id, amount, tx_name, tx_date_time FROM pay")
      // reader의 이름을 지정
      .name("jdbcCursorItemReader")
      .build();
  }

  private ItemWriter<Pay> jdbcCursorItemWriter() {
    return list -> {
      for (Pay pay: list) {
        log.info("Current Pay={}", pay);
      }
    };
  }
}