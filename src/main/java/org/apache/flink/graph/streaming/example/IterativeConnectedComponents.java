/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored. 
 * <p>
 * This implementation uses streaming iterations to asynchronously merge state among partitions.
 * For a single-pass implementation, see {@link ConnectedComponentsExample}.
 */
public class IterativeConnectedComponents implements ProgramDescription {


	private static final Logger LOG = LoggerFactory.getLogger(IterativeConnectedComponents.class);
	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple4<Long, Long, Long, Long>> edges = getEdgesDataSet(env);

		IterativeStream<Tuple4<Long, Long, Long, Long>> iteration = edges.iterate(15000);
		DataStream<Tuple4<Long, Long, Long, Long>> result = iteration.closeWith(
				iteration.keyBy(0).flatMap(new AssignComponents()));

		// Emit the results
		result.map(new MapFunction<Tuple4<Long,Long,Long,Long>, Tuple4<Long, Long, Long, Long>>() {
			@Override
			public Tuple4<Long, Long, Long, Long> map(Tuple4<Long, Long, Long, Long> value) throws Exception {
				if (LOG.isDebugEnabled()) {
					long totalTime = System.currentTimeMillis() - value.f2;
					// initial vertex, converge vertex value, number of iteration, total time, average time per iteration
					LOG.debug("{},{},{},{},{}", value.f0, value.f1, value.f3, totalTime, totalTime / value.f3);
				}
				return value;
			}
		}).print();

		env.execute("Streaming Connected Components");
	}

	@SuppressWarnings("serial")
	public static class AssignComponents extends RichFlatMapFunction<Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {

		private HashMap<Long, HashSet<Long>> components = new HashMap<>();

		@Override
		public void flatMap(Tuple4<Long, Long, Long, Long> edge, Collector<Tuple4<Long, Long, Long, Long>> out) {
			final long sourceId = edge.f0;
			final long targetId = edge.f1;
			final long sourceTime = edge.f2;
			final long iteration = edge.f3;
			long sourceComp = -1;
			long trgComp = -1;

			// check if the endpoints belong to existing components
			for (Entry<Long, HashSet<Long>> entry : components.entrySet()) {
				if ((sourceComp == -1) || (trgComp == -1)) {
					if (entry.getValue().contains(sourceId)) {
						sourceComp = entry.getKey();
					}
					if (entry.getValue().contains(targetId)) {
						trgComp = entry.getKey();
					}
				}
			}
			if (sourceComp != -1) {
				// the source belongs to an existing component
				if (trgComp != -1) {
					// merge the components
					merge(sourceComp, trgComp, sourceTime, iteration, out);
				}
				else {
					// add the target to the source's component
					// and update the component Id if needed
					addToExistingComponent(sourceComp, targetId, sourceTime, iteration, out);
				}
			}
			else {
				// the source doesn't belong to any component
				if (trgComp != -1) {
					// add the source to the target's component
					// and update the component Id if needed
					addToExistingComponent(trgComp, sourceId, sourceTime, iteration, out);
				}
				else {
					// neither src nor trg belong to any component
					// create a new component and add them in it
					createNewComponent(sourceId, targetId, sourceTime, iteration, out);
				}
			}
		}

		private void createNewComponent(long sourceId, long targetId, long sourceTime, long iteration, Collector<Tuple4<Long, Long, Long, Long>> out) {
			long componentId = Math.min(sourceId, targetId);
			HashSet<Long> vertexSet = new HashSet<>();
			vertexSet.add(sourceId);
			vertexSet.add(targetId);
			components.put(componentId, vertexSet);
			out.collect(new Tuple4<Long, Long, Long, Long>(sourceId, componentId, sourceTime, iteration + 1));
			out.collect(new Tuple4<Long, Long, Long, Long>(targetId, componentId, sourceTime, iteration + 1));
		}

		private void addToExistingComponent(long componentId, long toAdd, long sourceTime, long iteration, Collector<Tuple4<Long, Long, Long, Long>> out) {
			HashSet<Long> vertices = components.remove(componentId);
			if (componentId >= toAdd) {
				// output and update component ID
				for (long v: vertices) {
					out.collect(new Tuple4<Long, Long, Long, Long >(v, toAdd, sourceTime, iteration + 1));
				}
				vertices.add(toAdd);
				vertices.add(componentId);
				components.put(toAdd, vertices);
			}
			else {
				vertices.add(toAdd);
				components.put(componentId, vertices);
				out.collect(new Tuple4<Long, Long, Long, Long>(toAdd, componentId, sourceTime, iteration + 1));
			}
		}

		private void merge(long sourceComp, long trgComp, long sourceTime, long iteration, Collector<Tuple4<Long, Long, Long, Long>> out) {
			HashSet<Long> srcVertexSet = components.remove(sourceComp);
			HashSet<Long> trgVertexSet = components.remove(trgComp);
			long componentId = Math.min(sourceComp, trgComp);
			if (sourceComp == componentId) {
				// collect the trgVertexSet
				if (trgVertexSet!= null) {
					for (long v: trgVertexSet) {
						out.collect(new Tuple4<>(v, componentId, sourceTime, iteration + 1));
					}
				}
			}
			else {
				// collect the srcVertexSet
				if (srcVertexSet != null) {
					for (long v: srcVertexSet) {
						out.collect(new Tuple4<Long, Long, Long, Long>(v, componentId, sourceTime, iteration + 1));
					}
				}
			}
			if (trgVertexSet!= null) {
				srcVertexSet.addAll(trgVertexSet);
			}
			components.put(componentId, srcVertexSet);
		}
		
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 1) {
				System.err.println("Usage: ConnectedComponentsExample <input edges path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
		} else {
			System.out.println("Executing ConnectedComponentsExample example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ConnectedComponentsExample <input edges path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataStream<Tuple4<Long, Long, Long, Long>> getEdgesDataSet(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Tuple4<Long, Long, Long, Long>>() {
						@Override
						public Tuple4<Long, Long, Long, Long> map(String s) {
							String[] fields = s.split("\\t");
							long src = Long.parseLong(fields[0]);
							long trg = Long.parseLong(fields[1]);
							return new Tuple4<>(src, trg, System.currentTimeMillis(), 0L);
						}
					});
		}

		return env.generateSequence(1, 10).flatMap(
				new FlatMapFunction<Long, Tuple4<Long, Long, Long, Long>>() {
					@Override
					public void flatMap(Long key, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
						for (int i = 1; i < 3; i++) {
							long target = key + i;
							out.collect(new Tuple4<>(key, target, System.currentTimeMillis(), 0L) );
						}
					}
				});
	}

	@Override
	public String getDescription() {
		return "Streaming Connected Components";
	}
}
