package fr.training.spring.library.exposition.swagger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;

public class SwaggerResource {
    private static final Logger LOG = LoggerFactory.getLogger(SwaggerResource.class);

    /**
     * Default constructor
     */
    public SwaggerResource() {
        super();
    }

    @RequestMapping(value = "/")
    public String index() {
        LOG.info("swagger-ui.html");
        return "redirect:swagger-ui.html";
    }

}
