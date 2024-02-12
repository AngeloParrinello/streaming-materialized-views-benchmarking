package it.agilelab.thesis.nexmark.flink.source;

import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.generator.NexmarkGenerator;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link #NexmarkGeneratorSource} is a stateful
 * (the operations that remember information across multiple events i.e. window operator) source that emits events with exactly-once semantics.
 * It uses a generator to produce events and maintains its state through Flink's checkpointing mechanism, possibly
 * in parallel way. Exactly-once semantics require proper checkpointing to ensure that no data is lost in case
 * of failures: for this reason is important to implement the {@link CheckpointedFunction} interface.
 * <p>
 * Also remember that Flink needs to be aware of the state in order to make it fault tolerant
 * using checkpoints and savepoints. Moreover, the knowledge of the state is necessary to rescaling Flink
 * applications, meaning that Flink takes care of redistributing state across parallel instances.
 * <p>
 * For the source to be re-scalable, the first time the job is run, we precompute all the
 * elements that each of the tasks should emit and upon checkpointing, each element constitutes its
 * own partition. When rescaling, these partitions will be randomly re-assigned to the new tasks.
 * <p>
 * This scheme is called "Even-split redistribution". Each operator returns a List of state elements.
 * The whole state is logically a concatenation of all lists. On restore/redistribution, the list
 * is evenly divided into as many sublists as there are parallel operators. Each operator gets a sublist,
 * which can be empty, or contain one or more elements. As an example, if with parallelism 1 the checkpointed
 * state of an operator contains elements element1 and element2, when increasing the parallelism to 2,
 * element1 may end up in operator instance 0, while element2 will go to operator instance 1.
 * This strategy guarantees that each element will be emitted exactly-once, but elements will not
 * necessarily be emitted in ascending order, even for the same tasks.
 * <p>
 * Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate
 * that with the outside world. In this case see the org.apache.flink.api.common.state.CheckpointListener interface.
 */
public class NexmarkGeneratorSource extends RichParallelSourceFunction<NextEvent>
        implements CheckpointedFunction {
    private static final Logger LOGGER = NexmarkUtil.getLogger();
    /**
     * The number of events created so far.
     * <p>
     * Accumulators are simple constructs with an add operation and a final accumulated result,
     * which is available after the job ended. The most straightforward accumulator is a counter.
     */
    private final LongCounter eventsCreatedSoFar;
    /**
     * The configuration of the generator.
     */
    private GeneratorConfig config;
    /**
     * The number of elements emitted already. Used for exactly once semantics
     */
    private volatile long numElementsEmitted;
    /**
     * The generator used to generate events.
     */
    private NexmarkGenerator generator;
    /**
     * Flag to indicate whether source is still running and for job cancellation.
     */
    private volatile boolean running;
    /**
     * The state that is snapshotted every time a checkpoint is taken.
     */
    private transient ListState<Long> checkpointedState;

    /**
     * Creates a new source using the given generator configuration.
     *
     * @param config The configuration of the generator.
     */
    public NexmarkGeneratorSource(final GeneratorConfig config) {
        this.eventsCreatedSoFar = new LongCounter();
        this.config = config;
        this.running = true;
    }

    /**
     * Initialization method for the function. It is called before the actual working methods (like map or join)
     * and thus suitable for one time setup work. For functions that are part of an iteration, this method will be
     * invoked at the beginning of each iteration superstep.
     * <p>
     * The configuration object passed to the function can be used for configuration and initialization.
     * The configuration contains all parameters that were configured on the function in the program composition.
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the runtime catches
     *                   an exception, it aborts the task and lets the fail-over logic decide whether to retry the task execution.
     */
    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("eventsCreatedSoFar", this.eventsCreatedSoFar);
        LOGGER.info("Opening source with parallelism: " + getRuntimeContext().getNumberOfParallelSubtasks());
        LOGGER.info("Opening source with index: " + getRuntimeContext().getIndexOfThisSubtask());
        LOGGER.info("Opening source with: " + getRuntimeContext().getTaskNameWithSubtasks());
        // the first time the job is executed, we divide the elements among the tasks
        this.generator = new NexmarkGenerator(getSubGeneratorConfig());
        LOGGER.info("Opening source with config: " + this.config);
    }

    private GeneratorConfig getSubGeneratorConfig() {
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        int taskId = getRuntimeContext().getIndexOfThisSubtask();

        GeneratorConfig subGeneratorConfig = this.config.split(parallelism).get(taskId);
        this.config = subGeneratorConfig;

        return subGeneratorConfig;
    }

    /**
     * Starts the source. Implementations use the {@link SourceContext} to emit elements. Sources
     * that checkpoint their state for fault tolerance should use the {@link
     * SourceContext#getCheckpointLock()} checkpoint lock to ensure consistency between the
     * bookkeeping and emitting the elements.
     *
     * <p>Sources that implement {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} must lock on the {@link
     * SourceContext#getCheckpointLock()} checkpoint lock (using a synchronized
     * block) before updating internal state and emitting elements, to make both an atomic
     * operation.
     *
     * <p>Refer to the {@link org.apache.flink.streaming.api.functions.source.SourceFunction top-level class docs} for an example.
     * <p>
     * In order to make the updates to the state and output collection atomic
     * (required for exactly-once semantics on failure/recovery), the user is required to
     * get a lock from the source’s context.
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    @Override
    public void run(final SourceContext<NextEvent> ctx) {
        while (this.isRunning() && this.generator.hasNext()) {
            this.eventsCreatedSoFar.add(1);
            NextEvent nextEvent = this.generator.next();
            // output and state update are atomic
            synchronized (ctx.getCheckpointLock()) {
                this.numElementsEmitted = this.generator.getEventsCountSoFar();
                ctx.collect(nextEvent);
            }
        }
    }

    /**
     * Cancels the source. Most sources will have a while loop inside the {@link
     * #run(SourceContext)} method. The implementation needs to ensure that the source will break
     * out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>In case of an ungraceful shutdown (cancellation of the source operator, possibly for
     * failover), the thread that calls {@link #run(SourceContext)} will also be {@link
     * Thread#interrupt() interrupted}) by the Flink runtime, in order to speed up the cancellation
     * (to ensure threads exit blocking methods fast, like I/O, blocking queues, etc.). The
     * interruption happens strictly after this method has been called, so any interruption handler
     * can rely on the fact that this method has completed (for example to ignore exceptions that
     * happen after cancellation).
     *
     * <p>During graceful shutdown (for example stopping a job with a savepoint), the program must
     * cleanly exit the {@link #run(SourceContext)} method soon after this method was called. The
     * Flink runtime will NOT interrupt the source thread during graceful shutdown. Source
     * implementors must ensure that no thread interruption happens on any thread that emits records
     * through the {@code SourceContext} from the {@link #run(SourceContext)} method; otherwise the
     * clean shutdown may fail when threads are interrupted while processing the final records.
     *
     * <p>Because the {@code SourceFunction} cannot easily differentiate whether the shutdown should
     * be graceful or ungraceful, we recommend that implementors refrain from interrupting any
     * threads that interact with the {@code SourceContext} at all. You can rely on the Flink
     * runtime to interrupt the source thread in case of ungraceful cancellation. Any additionally
     * spawned threads that directly emit records through the {@code SourceContext} should use a
     * shutdown method that does not rely on thread interruption.
     */
    @Override
    public void cancel() {
        this.running = false;
    }

    /**
     * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to
     * the function to ensure that all state is exposed by means previously offered through {@link
     * FunctionInitializationContext} when the Function was initialized, or offered now by {@link
     * FunctionSnapshotContext} itself.
     * <p>
     * This method will be called by Flink every X seconds as configured (remember to enable checkpointing
     * when launch the job and set the interval time) and save the state value into the backend.
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception Thrown, if state could not be created ot restored.
     */
    @Override
    public void snapshotState(final FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.numElementsEmitted);

    }

    /**
     * This method is called when the parallel function instance is created during distributed
     * execution. Functions typically set up their state storing data structures in this method.
     * <p>
     * This method is called every time the user-defined function is initialized, be that when the function is
     * first initialized or be that when the function is actually recovering from an earlier checkpoint.
     * Given this, initializeState() is not only the place where different types of state are initialized,
     * but also where state recovery logic is included.
     * <p>
     * Overall, this method embodies two things:
     * <ol>
     *     <li>The initialization of the checkpointed state.</li>
     *     <li>The logic for restoring state from a checkpoint. Overall, the last code block is
     *     responsible for recovering the state of the {@link NexmarkGeneratorSource} after a checkpoint.
     *     It retrieves the checkpointed state, validates it, and then initializes a new instance\
     *     of the {@link NexmarkGeneratorSource} with the recovered state to continue generating
     *     events from the point where it left off before the failure.</li>
     * </ol>
     *
     * @param context the context for initializing the operator
     * @throws Exception Thrown, if state could not be created ot restored.
     */
    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>("nexmark-generator-source-state", LongSerializer.INSTANCE);

        this.checkpointedState = context.getOperatorStateStore().getListState(descriptor);


        if (context.isRestored()) {
            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // There is exactly one element in the retrievedStates list
            // given that the parallelism of the function is 1, we can only have 1 state

            // if we have multiple source, each source will have its own state
            // so there will be always 1 state per source
            Preconditions.checkArgument(retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            long numElementsToSkip = retrievedStates.get(0);
            this.generator = new NexmarkGenerator(getSubGeneratorConfig(), numElementsToSkip, 0);
        }
    }

    /**
     * Returns the number of events created so far.
     *
     * @return the number of events created so far
     */
    public LongCounter getEventsCreatedSoFar() {
        return this.eventsCreatedSoFar;
    }

    /**
     * Returns the generator used to generate events.
     *
     * @return the generator used to generate events
     */
    public NexmarkGenerator getGenerator() {
        return this.generator.copy();
    }

    /**
     * Returns the configuration of the generator.
     *
     * @return the configuration of the generator
     */
    public GeneratorConfig getConfig() {
        return this.config;
    }

    /**
     * Returns whether the source is still running.
     *
     * @return whether the source is still running
     */
    public boolean isRunning() {
        return this.running;
    }
}
