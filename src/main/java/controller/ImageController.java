package controller;

import dao.JobDao;
import listener.ImageRequestQueueListener;
import model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import service.JobService;
import service.SqsService;
import service.UploadService;

import java.util.List;

@RestController()
@RequestMapping(value = "/image-service-slave")
public class ImageController {

    private final static Logger LOGGER = LoggerFactory.getLogger(ImageController.class);

    private JobService jobService;
    private UploadService uploadService;
    private SqsService sqsService;
    private JobDao jobDao;
    private ImageRequestQueueListener imageRequestQueueListener;

    @Autowired
    public ImageController(JobService jobService, UploadService uploadService, SqsService sqsService,
                           JobDao jobDao, ImageRequestQueueListener imageRequestQueueListener) {
        this.jobService = jobService;
        this.uploadService = uploadService;
        this.sqsService = sqsService;
        this.jobDao = jobDao;
        this.imageRequestQueueListener = imageRequestQueueListener;

        Thread imageRequestListenerThread = new Thread(imageRequestQueueListener);
        imageRequestListenerThread.start();
    }

    @RequestMapping(method = RequestMethod.GET, value = "/")
    String home() {
        return "Image Service Slave is running.";
    }


}
