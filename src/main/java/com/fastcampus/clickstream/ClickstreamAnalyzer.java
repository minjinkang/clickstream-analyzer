package com.fastcampus.clickstream;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ClickstreamAnalyzer {
    public enum DataType {
        // 활성세션정보, 초당 광고클릭수, 초당 요청 수, 초당 에러 카운트 수
        ACTIVE_SESSION, ADS_PER_SECOND, REQUEST_PER_SECOND, ERROR_PER_SECOND
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(4);

        // 1. flink에서 카프카 소스로부터 로그정보를 읽어오는 로직
        KafkaSource<String> source = KafkaSource.<String>builder()
                // 로컬을 생성하는 카프카 정보
                .setBootstrapServers("localhost:9092")
                // 토픽이름, 그룹아이디, 카프라로부터 데이터를 어떻게 얻어올것인가, 
                // 어떠한 시리얼라이저를 사용할 것인가
                .setTopics("weblog")
                .setGroupId("test1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build(); // 카프카소스 생성

        // 데이터스트림 생성
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");
        // 2. webLog의 코드 형태로 변환
        DataStream<WebLog> webLogDataStream = dataStream.map(new WebLogMapFunction());
        webLogDataStream.print();

        // 3. 해당 webLog 데이터스트림으로부터 활성세션정보를 카운트하는 처리
        // 변환된 웹로그를 기반으로 현재 활성중인 세션수에 대한 정보를 계산
        // 활성세션카운트 정보를 웹로그로부터 키바이를 통해 하나의 파티션으로 모여 전달이 된다.
        // 해당결과를 outmapfunction을 통해 데이터베이스에 저장
        DataStream<Tuple2<Long, Map<DataType, Integer>>> activeSessionDataStream = webLogDataStream
                .keyBy(t -> 1)
                .process(new ActiveSessionCountFunction())
                // 출력된 결과를 데이터베이스에 저장
                .map(new OutputMapFunction(DataType.ACTIVE_SESSION));

        // 4. 초당 광고클릭수 카운트 처리
        DataStream<Tuple2<Long, Map<DataType, Integer>>> adsClickPerSecondDataStream = webLogDataStream
                // 실제 광고 url을 클릭한 데이터만 처리하는 filter 조건을 걸어줌
                .filter(l -> l.getUrl().startsWith("/ads"))
                .keyBy(t -> 1)
                .process(new RequestPerSecondFunction())
                .map(new OutputMapFunction(DataType.ADS_PER_SECOND));
        // 5. 초당 요청 횟수 
        DataStream<Tuple2<Long, Map<DataType, Integer>>> requestPerSecondDataStream = webLogDataStream
                .keyBy(t -> 1)
                .process(new RequestPerSecondFunction())
                .map(new OutputMapFunction(DataType.REQUEST_PER_SECOND));
        // 6. 초당 에러 횟수를 합쳐 
        DataStream<Tuple2<Long, Map<DataType, Integer>>> errorPerSecondDataStream = webLogDataStream
                // 응답시간이 400초이상이면 에러처리 filter 조건
                .filter(l -> Integer.parseInt(l.getResponseCode()) >= 400)
                .keyBy(t -> 1)
                .process(new RequestPerSecondFunction())
                .map(new OutputMapFunction(DataType.ERROR_PER_SECOND));
        // 7. 최종결과로 생성
        DataStream<Tuple2<Long, Map<DataType, Integer>>> resultDataStream = activeSessionDataStream
                .union(adsClickPerSecondDataStream)
                .union(requestPerSecondDataStream)
                .union(errorPerSecondDataStream)
                .keyBy(t -> t.f0)
                .reduce((value1, value2) -> {
                    value2.f1.forEach((key, value) -> value1.f1.merge(key, value, (v1, v2) -> v1 >= v2 ? v1 : v2));
                    return value1;
                });
        resultDataStream.print();

        // 8. mysql 데이터베이스에 저장
        resultDataStream.addSink(
                JdbcSink.sink(
                        "REPLACE INTO stats (ts, active_session, ads_per_second, request_per_second, error_per_second) values (?, ?, ?, ?, ?)",
                        (statement, tuple) -> {
                            Timestamp timestamp = new Timestamp(tuple.f0);
                            statement.setTimestamp(1, timestamp);
                            statement.setInt(2, tuple.f1.getOrDefault(DataType.ACTIVE_SESSION, 0));
                            statement.setInt(3, tuple.f1.getOrDefault(DataType.ADS_PER_SECOND, 0));
                            statement.setInt(4, tuple.f1.getOrDefault(DataType.REQUEST_PER_SECOND, 0));
                            statement.setInt(5, tuple.f1.getOrDefault(DataType.ERROR_PER_SECOND, 0));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(200)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/clickstream")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("1234")
                                .build()));

        env.execute("Clickstream Analyzer");
    }

    public static class WebLogMapFunction implements MapFunction<String, WebLog> {
        @Override
        public WebLog map(String value) throws Exception {
            // kafka로부터 들어오는 value 값을 웹로그클래스의 형태로 변환
            // 띄어쓰기로 자른 token 정보
            String[] tokens = value.split(" ");
            return new WebLog(tokens[0], Instant.parse(tokens[1]).toEpochMilli(), tokens[2], tokens[3], tokens[4],
                    tokens[5], tokens[6]);
        }
    }

        // 현재 활성중인 세션에 대한 정보를 카운트를 계산
    public static class ActiveSessionCountFunction
            extends KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>> {
        // 세션정보를 저장
        private transient MapState<String, Long> sessionMapState;
        // 초기 타이머 등록
        private transient ValueState<Long> timerValueState;
        // 1초단위로 결과를 출력할 것
        private static final long INTERVAL = 1000;
        // 30초이상 클릭을 발생하지않으면 세션이 종료될 시간을 정해줌
        private static final long SESSION_TIMEOUT = 30 * 1000;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("sessionMap", String.class,
                    Long.class);
            sessionMapState = getRuntimeContext().getMapState(mapStateDescriptor);

            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("firetime",
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            timerValueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        // kafka로부터 입력된 로그의 처리결과를 MapState에 업데이트한다
        @Override
        public void processElement(WebLog log,
                KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>>.Context ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {
            //  요청한 세션아이디가 타임아웃보다 작은경우에 세션유지되고있기때문에 업데이트
            if (System.currentTimeMillis() - log.getTimestamp() <= SESSION_TIMEOUT) {
                sessionMapState.put(log.getSessionId(), log.getTimestamp());
            }

            long timestamp = ctx.timerService().currentProcessingTime();
            // 첫 레코드를 기준으로 타임스탬프 정보 등록
            if (null == timerValueState.value()) {
                long nextTimerTimestamp = timestamp - (timestamp % INTERVAL) + INTERVAL;
                timerValueState.update(nextTimerTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextTimerTimestamp);
            }
        }
        @Override
        public void onTimer(long timestamp,
                KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>>.OnTimerContext ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {
            int size = 0;
            for (Map.Entry<String, Long> session : sessionMapState.entries()) {
                // 현재 시간에서 세션상태에 저장된 타임스탬프 정보를 뺏을때 세션 타임아웃 보다 작으면 = 현재 활성중인 세션임 타이머증가
                if (System.currentTimeMillis() - session.getValue() <= SESSION_TIMEOUT) {
                    size++;
                }
            }

            long currentProcessingTime = (ctx.timerService().currentProcessingTime() / 1000) * 1000;
            out.collect(Tuple2.of(currentProcessingTime, size));

            long nextTimerTimestamp = timestamp + INTERVAL;
            ctx.timerService().registerProcessingTimeTimer(nextTimerTimestamp);
        }
    }

    public static class OutputMapFunction
            implements MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Map<DataType, Integer>>> {
        private DataType dataType;

        public OutputMapFunction(DataType dataType) {
            this.dataType = dataType;
        }

        // map 함수
        @Override
        public Tuple2<Long, Map<DataType, Integer>> map(Tuple2<Long, Integer> value) throws Exception {
            Map<DataType, Integer> map = new HashMap<>();
            map.put(dataType, value.f1); // 키와 밸류 정보
            return Tuple2.of(value.f0, map);
        }
    }


    public static class RequestPerSecondFunction extends KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>> {
        private transient ValueState<Long> timerValueState;
        private transient ValueState<Integer> countState;
        private static final long INTERVAL = 1000;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("firetime",
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            timerValueState = getRuntimeContext().getState(valueStateDescriptor);

            ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("requestCount",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }));
            countState = getRuntimeContext().getState(countStateDescriptor);
        }

        @Override
        public void processElement(WebLog value,
                KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>>.Context ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {
            long timestamp = ctx.timerService().currentProcessingTime();
            long nextTimerTimestamp = timestamp - (timestamp % INTERVAL) + INTERVAL;
            if (timerValueState.value() == null) {
                timerValueState.update(nextTimerTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextTimerTimestamp);
            }

            if (countState.value() == null) {
                countState.update(0);
            }

            if (nextTimerTimestamp - INTERVAL <= value.getTimestamp() && value.getTimestamp() < nextTimerTimestamp) {
                countState.update(countState.value() + 1);
            }
        }

        @Override
        public void onTimer(long timestamp,
                KeyedProcessFunction<Integer, WebLog, Tuple2<Long, Integer>>.OnTimerContext ctx,
                Collector<Tuple2<Long, Integer>> out) throws Exception {
            long currentProcessingTime = (ctx.timerService().currentProcessingTime() / 1000) * 1000;

            out.collect(Tuple2.of(currentProcessingTime, countState.value()));
            countState.update(0);

            long nextTimerTimestamp = timestamp + INTERVAL;
            ctx.timerService().registerProcessingTimeTimer(nextTimerTimestamp);
        }
    }
}