package fr.training.spring.library.exposition.swagger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(PathSelectors.any())
                .build();
    }

    //    ApiInfo apiInfo() {
//        return new ApiInfoBuilder()//
//                .title("Swagger Sample-service") //
//                .description("No description provided") //
//                .license("Apache 2.0") //
//                .licenseUrl("http://www.apache.org/licenses/LICENSE-2.0.html") //
//                .termsOfServiceUrl("") //
//                .version("1.0") //
//                .contact(new Contact("Name", "http://perdu.com", "SomeOne@acme.fr")) //
//                .build();
//    }
}