package listener;

import com.amazonaws.services.sqs.model.Message;
import constants.ServiceConstants;
import dao.JobDao;
import model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import service.BashExecuterService;
import service.SqsService;

import java.util.List;

@Component
public class ImageRequestQueueListener implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ImageRequestQueueListener.class);

    private SqsService sqsService;

    private BashExecuterService bashExecuterService;

    private JobDao jobDao;

    public ImageRequestQueueListener() {
    }

    @Autowired
    public ImageRequestQueueListener(SqsService sqsService,
                                     BashExecuterService bashExecuterService, JobDao jobDao) {
        this.sqsService = sqsService;
        this.bashExecuterService = bashExecuterService;
        this.jobDao = jobDao;
    }

    @Override
    public void run() {
        int count = 0;
        while (true) {
            LOGGER.info("Polling SQS for messages.");
            List<Message> messages = null;

            if (sqsService != null) {
                messages = sqsService.getMessages();

                if (CollectionUtils.isEmpty(messages)) {
                    try {
                        if (count >= 3) {
                            LOGGER.info("Shutting down instance.");
                            bashExecuterService.shutDownInstance();
                        }
                        LOGGER.info("ImageRequestQueueListener thread sleeping for time={}ms.", ServiceConstants.SLEEP_TIME_20S);
                        count++;
                        Thread.sleep(ServiceConstants.SLEEP_TIME_20S);
                    } catch (InterruptedException e) {
                        LOGGER.error(e.getMessage());
                    }
                } else {
                    String messageBody = messages.get(0).getBody();
                    LOGGER.info("Message body={}", messageBody);

                    // Fetches job record from MongoDB.
                    Job job = jobDao.getJob(messageBody);

                    // Execute bash script to recognize image.
                    String result = bashExecuterService.recognizeImage(job.getUrl());
                    LOGGER.info("Result computed for jobId={}, result={}", job.getId(), result);
                    job.setResult(result);

                    // Updates job record in MongoDB.
                    jobDao.updateJob(job);

                    // Deletes message from the queue.
                    String messageReceiptHandle = messages.get(0).getReceiptHandle();
                    sqsService.deleteMessage(messageReceiptHandle);

                    try {
                        LOGGER.info("ImageRequestQueueListener thread sleeping for time={}ms.", ServiceConstants.SLEEP_TIME_1S);
                        Thread.sleep(ServiceConstants.SLEEP_TIME_1S);
                    } catch (InterruptedException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        }

    }

}
