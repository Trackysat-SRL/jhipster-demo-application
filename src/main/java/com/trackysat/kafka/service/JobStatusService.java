package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.JobStatus;
import com.trackysat.kafka.repository.JobStatusRepository;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing jobStatus.
 */
@Service
public class JobStatusService {

    private final Logger log = LoggerFactory.getLogger(JobStatusService.class);

    private final String DAILY_PROCESS_JOB = "daily_process";

    private final String MONTHLY_PROCESS_JOB = "monthly_process";

    private final JobStatusRepository jobStatusRepository;

    public JobStatusService(JobStatusRepository jobStatusRepository) {
        this.jobStatusRepository = jobStatusRepository;
    }

    public Optional<JobStatus> getOne(String id) {
        return jobStatusRepository.findById(id);
    }

    public List<JobStatus> getAll() {
        return jobStatusRepository.findAll();
    }

    public Optional<Instant> getLastDayProcessed() {
        return this.getOne(DAILY_PROCESS_JOB).map(JobStatus::getUpdatedDate);
    }

    public void setLastDayProcessed(LocalDate day, String data) {
        this.setLastProcessed(DAILY_PROCESS_JOB, day, data);
    }

    public Optional<Instant> getLastMonthProcessed() {
        return this.getOne(MONTHLY_PROCESS_JOB).map(JobStatus::getUpdatedDate);
    }

    public void setLastMonthProcessed(LocalDate day, String data) {
        this.setLastProcessed(MONTHLY_PROCESS_JOB, day, data);
    }

    public void setLastProcessed(String jobId, LocalDate day, String data) {
        JobStatus jobStatusUpdated = new JobStatus();
        jobStatusUpdated.setJobId(jobId);
        jobStatusUpdated.setUpdatedDate(day.atStartOfDay().toInstant(ZoneOffset.UTC));
        jobStatusUpdated.setData(data);
        this.jobStatusRepository.save(jobStatusUpdated);
    }
}
