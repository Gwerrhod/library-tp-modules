package fr.training.spring.library.batch.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import fr.training.spring.library.batch.common.FullReportListener;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;


@Configuration
public class JobConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(JobConfiguration.class);

	private String[] Headers = {"id", "type", "addressNumber", "addressStreet",  "addressPostalCode",
			"addressCity","directorSurname","directorName"};

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private FullReportListener jobListener;

	@Autowired
	public DataSource dataSource;

	@Bean
	public Job exportJob(final Step step1) {
		return jobBuilderFactory.get("jobExport") //
				.incrementer(new RunIdIncrementer()) // job can be launched as many times as desired
				.start(step1) //
				.listener(jobListener) //
				.build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1") //
				.tasklet(new Tasklet() {
					@Override
					public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext)
							throws Exception {
						logger.info("Hello Spring batch !");
						return RepeatStatus.FINISHED;
					}
				}).build();
	}

	/**
	 * ItemReader is an abstract representation of how data is provided as input to
	 * a Step. When the inputs are exhausted, the ItemReader returns null.
	 */
	@Bean
	public JdbcCursorItemReader<LibraryDTO> exportReader() {
		final JdbcCursorItemReader<LibraryDTO> reader = new JdbcCursorItemReader<LibraryDTO>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT id, type, " +
				"addressNumber, addressStreet,  addressPostalCode" +
				"addressCity,directorSurname,directorName"+
				"FROM Library");
		reader.setRowMapper(new LibraryRowMapper());

		return reader;
	}

	/**
	 * RowMapper used to map resultset to OrderDto
	 */
	public class LibraryRowMapper implements RowMapper<LibraryDTO> {

		@Override
		public LibraryDTO mapRow(final ResultSet rs, final int rowNum) throws SQLException {
			final LibraryDTO libraryDto = new LibraryDTO();
			libraryDto.setId(rs.getString("id"));
			libraryDto.setType(rs.getString("type"));
			libraryDto.setAddressNumber(rs.getString("addressNumber"));
			libraryDto.setAddressStreet(rs.getString("addressStreet"));
			libraryDto.setAddressPostalCode(rs.getString("addressPostalCode"));
			libraryDto.setAddressCity(rs.getString("addressCity"));
			libraryDto.setDirectorSurname(rs.getString("directorSurname"));
			libraryDto.setDirectorName(rs.getString("directorName"));
			return libraryDto;
		}
	}

	@Bean
	public ItemProcessor<LibraryDTO, LibraryDTO> exportProcessor() {
		return new ItemProcessor<LibraryDTO, LibraryDTO>() {

			@Override
			public LibraryDTO process(final LibraryDTO library) throws Exception {
				logger.info("Processing {}", library);
				return library;
			}
		};
	}

	@StepScope // Mandatory for using jobParameters
	@Bean
	public FlatFileItemWriter<LibraryDTO> exportWriter(@Value("#{jobParameters['output-file']}") final String outputFile) {
		final FlatFileItemWriter<LibraryDTO> writer = new FlatFileItemWriter<LibraryDTO>();
		writer.setResource(new FileSystemResource(outputFile));
		final DelimitedLineAggregator<LibraryDTO> lineAggregator = new DelimitedLineAggregator<LibraryDTO>();
		final BeanWrapperFieldExtractor<LibraryDTO> fieldExtractor = new BeanWrapperFieldExtractor<LibraryDTO>();
		fieldExtractor.setNames(Headers);
		lineAggregator.setFieldExtractor(fieldExtractor);
		lineAggregator.setDelimiter(";");
		writer.setLineAggregator(lineAggregator);
		writer.setHeaderCallback(new FlatFileHeaderCallback() {
			@Override
			public void writeHeader(final Writer writer) throws IOException {
				writer.write(String.valueOf(Headers));
			}
		});
		return writer;
	}

}
