package com.allardworks.workinator3.coordinator.mongodb.testsupport;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DummyDelegateWorker implements AsyncWorker {
    private final Consumer<WorkerContext> contextMethod;

    //@Override
    public void execute(WorkerContext context) {
        contextMethod.accept(context);
    }

    //@Override
    public void close() {

    }
}
