package com.batch.project.job.processjob;

import com.batch.project.entity.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class ProcessorNullJobConfig {

  public static final String JOB_NAME = "processorNullBatch";
  public static final String BEAN_PREFIX = JOB_NAME + "_";

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final EntityManagerFactory emf;

  @Value("${chunkSize:1000}")
  private int chunkSize;

  @Bean(JOB_NAME)
  public Job job() {
    return jobBuilderFactory.get(JOB_NAME)
      .preventRestart()
      .start(step())
      .build();
  }

  @Bean(BEAN_PREFIX + "step")
  @JobScope
  public Step step() {
    return stepBuilderFactory.get(BEAN_PREFIX + "step")
      .<Pay, Pay>chunk(chunkSize)
      .reader(reader())
      .processor(processor())
      .writer(writer())
      .build();
  }

  @Bean
  public JpaPagingItemReader<Pay> reader() {
    return new JpaPagingItemReaderBuilder<Pay>()
      .name(BEAN_PREFIX + "reader")
      .entityManagerFactory(emf)
      .pageSize(chunkSize)
      .queryString("SELECT p FROM Pay p")
      .build();
  }

  @Bean
  public ItemProcessor<Pay, Pay> processor() {
    return teacher -> {
      boolean isIgnoreTarget = teacher.getId() % 2 == 0L;
      if (isIgnoreTarget) {
        log.info(">>>>>>>>> Teacher name={}, isIgnoreTarget={}", teacher.getTxName(), isIgnoreTarget);
        return null;
      }

      return teacher;
    };
  }

  private ItemWriter<Pay> writer() {
    return items -> {
      for (Pay item : items) {
        log.info("Teacher Name={}", item.getTxName());
      }
    };
  }
}