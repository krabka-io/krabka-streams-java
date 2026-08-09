/**
 * In-memory columnar and schema-registry test helpers.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var driver = new ColumnarTestDriver(topology.build());
 * driver.pipeInput("input", 0, key, value, 0L);
 * assertThat(driver.readOutput("output"))
 *     .usingRecursiveComparison()
 *     .isEqualTo(expected);
 * }</pre>
 */
package io.krabka.streams.test;
