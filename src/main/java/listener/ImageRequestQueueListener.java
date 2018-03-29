package listener;

import com.amazonaws.services.sqs.model.Message;
import dao.JobDao;
import model.ImageRecognitionResult;
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

    @Value("${app.auto.shutdown.enabled}")
    private boolean isAutoShutdownEnabled;

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
        LOGGER.info("ImageRequestQueueListener : Polling request SQS for messages, shutdownCounter={}", count);
        List<Message> messages = null;

        messages = sqsService.getMessages(this.requestQueueName);

        if (CollectionUtils.isEmpty(messages)) {
            try {
                if (isAutoShutdownEnabled && count >= 1) {
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
            count = 0;
            String messageBody = messages.get(0).getBody();
            LOGGER.info("ImageRequestQueueListener : Request queue Message body={}", messageBody);

            // Fetches job record from MongoDB.
            Job job = jobDao.getJob(messageBody);
            ImageRecognitionResult result = null;

            if (job == null) {
                LOGGER.info("ImageRequestQueueListener : Unable to retrieve job with id={} record from Mongo,", messageBody);
            } else {
                // Execute bash script to recognize image.
                result = bashExecuterService.recognizeImage(job.getUrl());

                if (result.getResult() == null) {
                    LOGGER.error("ImageRequestQueueListener : Result computation for jobId={} failed.", messageBody);
                    job.setStatus(JobStatus.FAILED);
                } else {
                    LOGGER.info("ImageRequestQueueListener : Result computed for jobId={}, result={}", job.getId(), result.getResult());
                    job.setCompletedDateTime(DateTime.now(DateTimeZone.UTC));
                    job.setStatus(JobStatus.COMPLETE);

                    // Deletes message from the queue on successful result computation.
                    String messageReceiptHandle = messages.get(0).getReceiptHandle();
                    sqsService.deleteMessage(messageReceiptHandle, this.requestQueueName);

                    String resultString = "[" + job.getInputFilename() + "," + result.getResult().split("\\(score")[0] + "]";

                    //Appends to a file (actually replaces the file for every request). Doesn't ensure correctness of concurrent requests.
//                    uploadService.uploadResultToS3(resultString);
                    //Writes as key value pairs to a different bucket. Key = resultString and content also is resultString here.
                    uploadService.putResultAsKeyValuePairs(job.getInputFilename(), result.getResult().split("\\(score")[0]);
                }

                // Updates job record in MongoDB.
                job.setResult(result.getResult());
                job.setError(result.getError());
                jobDao.updateJob(job);


                try {
                    LOGGER.info("ImageRequestQueueListener : thread sleeping for time={}ms.", this.minSleepTime);
                    Thread.sleep(this.minSleepTime);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }

        }

    }
}
