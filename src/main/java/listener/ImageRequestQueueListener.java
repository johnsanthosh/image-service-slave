package listener;

import com.amazonaws.services.sqs.model.Message;
import dao.JobDao;
import model.Job;
import model.JobStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import service.BashExecuterService;
import service.SqsService;
import service.UploadService;

import java.util.List;

@Component
public class ImageRequestQueueListener implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ImageRequestQueueListener.class);

    private SqsService sqsService;

    private BashExecuterService bashExecuterService;

    private JobDao jobDao;

    private UploadService uploadService;

    @Value("${amazon.sqs.request.queue.name}")
    private String requestQueueName;

    @Value("${amazon.sqs.request.queue.message.group.id}")
    private String requestQueueGroupId;

    @Value("${amazon.sqs.instance.shutdown.queue.name}")
    private String instanceShutdownQueueName;

    @Value("${amazon.sqs.instance.shutdown.queue.message.group.id}")
    private String instanceShutdownQueueGroupId;

    @Value("${sleep.time.max}")
    private int maxSleepTime;

    @Value("${sleep.time.min}")
    private int minSleepTime;

    private static int count = 0;

    public ImageRequestQueueListener() {
    }

    @Autowired
    public ImageRequestQueueListener(SqsService sqsService, BashExecuterService bashExecuterService,
                                     JobDao jobDao, UploadService uploadService) {
        this.sqsService = sqsService;
        this.bashExecuterService = bashExecuterService;
        this.jobDao = jobDao;
        this.uploadService = uploadService;
    }

    @Override
    public void run() {
        while (true) {

            if (sqsService != null) {
                pollRequestQueue();
            }
        }

    }

    private void pollRequestQueue() {
        LOGGER.info("ImageRequestQueueListener : Polling request SQS for messages.");
        List<Message> messages = null;

        messages = sqsService.getMessages(this.requestQueueName);

        if (CollectionUtils.isEmpty(messages)) {
            try {
                LOGGER.info("ImageRequestQueueListener : count={}", count);
                if (count >= 3) {
                    LOGGER.info("ImageRequestQueueListener : Shutting down instance.");
                    bashExecuterService.shutDownInstance();
                    sqsService.insertToQueue("Instance shutting down.", this.instanceShutdownQueueName);
                }
                LOGGER.info("ImageRequestQueueListener : thread sleeping for time={}ms.", this.maxSleepTime);
                count++;
                Thread.sleep(this.maxSleepTime);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }
        } else {

            String messageBody = messages.get(0).getBody();
            LOGGER.info("ImageRequestQueueListener : Request queue Message body={}", messageBody);

            // Fetches job record from MongoDB.
            Job job = jobDao.getJob(messageBody);

            // Execute bash script to recognize image.
            String result = bashExecuterService.recognizeImage(job.getUrl());

            if (StringUtils.isEmpty(result)) {
                LOGGER.info("ImageRequestQueueListener : Result computation for jobId={} failed.", job.getId());
                job.setResult(result);
                job.setStatus(JobStatus.FAILED);
            } else {
                LOGGER.info("ImageRequestQueueListener : Result computed for jobId={}, result={}", job.getId(), result);
                job.setCompletedDateTime(DateTime.now(DateTimeZone.UTC));
                job.setStatus(JobStatus.COMPLETE);

                // Deletes message from the queue on successful result computation.
                String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqsService.deleteMessage(messageReceiptHandle, this.requestQueueName);

                String resultString = "[" + job.getInputFilename() + "," + result.split("\\(score")[0] + "]";
                uploadService.uploadResultToS3(resultString);
            }

            // Updates job record in MongoDB.
            jobDao.updateJob(job);


            try {
                LOGGER.info("ImageRequestQueueListener : thread sleeping for time={}ms.", this.minSleepTime);
                Thread.sleep(this.minSleepTime);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }
        }

    }

    void pushMessageToInstanceShutdownQueue() {

    }

}
