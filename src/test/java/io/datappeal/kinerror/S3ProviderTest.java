package io.datappeal.kinerror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.datappeal.kinerror.model.Error;
import io.datappeal.kinerror.s3.S3Provider;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ProviderTest {

    private AmazonS3 amazonS3;
    private S3Provider s3Provider;

    @Before
    public void setUp() {

        this.amazonS3 = mock(AmazonS3.class);
        this.s3Provider = new S3Provider(this.amazonS3);

    }


    @Test
    public void shouldListingErrorsKeys() {

        final String bucketName = "myBucket";
        final String prefix = "myPrefix";

        ListObjectsV2Result objectListing = new ListObjectsV2Result();
        objectListing.setBucketName(bucketName);
        objectListing.setPrefix(prefix);


        final S3ObjectSummary s3ObjectSummary1 = new S3ObjectSummary();
        final S3ObjectSummary s3ObjectSummary2 = new S3ObjectSummary();
        final S3ObjectSummary s3ObjectSummary3 = new S3ObjectSummary();

        s3ObjectSummary1.setKey("myKey1");
        s3ObjectSummary2.setKey("myKey2");
        s3ObjectSummary3.setKey("myKey3");

        objectListing.getObjectSummaries().add(s3ObjectSummary1);
        objectListing.getObjectSummaries().add(s3ObjectSummary2);
        objectListing.getObjectSummaries().add(s3ObjectSummary3);


        when(this.amazonS3.listObjectsV2(bucketName, prefix)).thenAnswer(i -> objectListing);

        List<String> strings = this.s3Provider.listErrorsKeys(bucketName, prefix);


        assertThat(strings).containsExactlyInAnyOrder("myKey1", "myKey2", "myKey3");

    }


    @Test
    public void shouldGetError() throws IOException {

        final String bucketName = "myBucket";
        final String key = "myKey";

        InputStream targetStream = TestResources.getResource("data/urls-prod-2020-04-01-23-39-13-08b053c8-4aa6-45bb-bafc-a6bae0945dc8.json");

        final S3Object s3Object = new S3Object();
        s3Object.setBucketName(bucketName);
        s3Object.setKey(key);
        s3Object.setObjectContent(targetStream);


        when(this.amazonS3.getObject(bucketName, key)).thenAnswer(i -> s3Object);

        final Error error = this.s3Provider.getError(bucketName, key);

        assertThat(error.getEntries()).hasSize(11);

    }


    @Test
    public void shouldGetDataFromBatchedFile() throws IOException {

        final String bucketName = "myBucket";
        final String key = "myKey";

        InputStream targetStream = TestResources.getResource("data/urls-prod-58-2020-03-15-05-00-58-23ac1aa7-c186-4f35-8966-9513c2dbc449.json");

        final S3Object s3Object = new S3Object();
        s3Object.setBucketName(bucketName);
        s3Object.setKey(key);
        s3Object.setObjectContent(targetStream);


        when(this.amazonS3.getObject(bucketName, key)).thenAnswer(i -> s3Object);

        List<String> data = this.s3Provider.getData(bucketName, key);

        assertThat(data).hasSize(1299);
        assertThat(data.get(0)).isEqualTo("{\"url\":\"https://www.tripadvisor.it/Restaurant_Review-g187870-d12811841-Reviews-Amorino-Venice_Veneto.html\",\"location_latitude\":45.43879,\"location_longitude\":12.334315,\"reviews_count\":150,\"price_indicator_min\":2.0,\"price_indicator_max\":3.0,\"price_indicator_currency_staging\":\"EUR\",\"category_staging\":\"Pasticcerie E Gelaterie\",\"ranking_index\":49,\"ranking_total\":106,\"ranking_category_staging\":\"Dessert\",\"ranking_location_staging\":\"Venice\",\"distribution_rating_5\":85,\"distribution_rating_4\":31,\"distribution_rating_3\":10,\"distribution_rating_2\":10,\"distribution_rating_1\":14,\"ratings_general\":4,\"ratings_quality_price\":4,\"ratings_service\":4,\"ratings_food\":4,\"max_rating_general\":5,\"special_offers\":false,\"red_banner\":false,\"verified_by_network\":false,\"certificate_of_excellence\":false,\"closed\":false,\"url_id\":5894493,\"hostname_staging\":\"worker-7hsj4\",\"timestamp_crawling\":\"2020-03-15 05:00:27.0\",\"progressive_id\":333,\"network_staging\":\"tripadvisor\",\"not_found\":false}");
        assertThat(data.get(1298)).isEqualTo("{\"url\":\"https://www.expedia.it/Carate-Urio-Hotels-Residenza-Firenze.h31937334.Informazioni-Hotel\",\"location_latitude\":45.87245,\"location_longitude\":9.12512,\"reviews_count\":0,\"distribution_user_type_family\":0,\"distribution_user_type_couple\":0,\"distribution_user_type_solo\":0,\"distribution_user_type_business\":0,\"distribution_user_type_friends\":0,\"distribution_user_type_others\":0,\"ratings_general\":0,\"ratings_hotel\":0,\"ratings_position\":0,\"ratings_rooms\":0,\"ratings_service\":0,\"ratings_cleaning\":0,\"ratings_comfort\":0,\"ratings_quality_price\":0,\"ratings_neighborhood\":0,\"max_rating_general\":5,\"url_id\":5682138,\"hostname_staging\":\"worker-7thnx\",\"timestamp_crawling\":\"2020-03-15 05:05:48.0\",\"progressive_id\":117,\"network_staging\":\"ebookers\",\"not_found\":false}");


    }
}
