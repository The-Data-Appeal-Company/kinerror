package io.datappeal.kinerror;

import org.apache.commons.collections4.ListUtils;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTest {

    @Test
    public void shouldPartitionProperly() {

        List<Integer> range = IntStream.rangeClosed(1, 25)
                .boxed().collect(Collectors.toList());

        List<List<Integer>> partition = ListUtils.partition(range, 4);

        assertThat(partition).hasSize(7);
        assertThat(partition.get(6)).hasSize(1);


    }
}
