package io.trino.velox;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class NativeTaskConfig
{
    // Native Log verbose level, 'trino_bridge=1,TrinoExchangeSource=2' means that will make
    // the verbose log level in trino_bridge be 1, and the verbose log level in TrinoExchangeSource be 2.
    private String logVerboseModules = "";

    @NotNull
    public String getLogVerboseModules()
    {
        return logVerboseModules;
    }

    @Config("native.log-verbose-modules")
    public NativeTaskConfig setLogVerboseModules(String logVerboseModules)
    {
        this.logVerboseModules = logVerboseModules;
        return this;
    }
}
