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

    public Optional<Instant> getLastDayProcessed(String deviceId) {
        return this.getOne(deviceId + "_" + DAILY_PROCESS_JOB).map(JobStatus::getUpdatedDate);
    }

    public JobStatus setLastDayProcessed(String deviceId, LocalDate day, String data) {
        return this.setLastProcessed(deviceId + "_" + DAILY_PROCESS_JOB, day, data);
    }

    public Optional<Instant> getLastMonthProcessed(String deviceId) {
        return this.getOne(deviceId + "_" + MONTHLY_PROCESS_JOB).map(JobStatus::getUpdatedDate);
    }

    public JobStatus setLastMonthProcessed(String deviceId, LocalDate day, String data) {
        return this.setLastProcessed(deviceId + "_" + MONTHLY_PROCESS_JOB, day, data);
    }

    public JobStatus setLastProcessed(String jobId, LocalDate day, String data) {
        JobStatus jobStatusUpdated = new JobStatus();
        jobStatusUpdated.setJobId(jobId);
        jobStatusUpdated.setUpdatedDate(day.atStartOfDay().toInstant(ZoneOffset.UTC));
        jobStatusUpdated.setData(data);
        return this.jobStatusRepository.save(jobStatusUpdated);
    }

    public boolean deleteOne(String id) {
        return jobStatusRepository.delete(id);
    }
}
