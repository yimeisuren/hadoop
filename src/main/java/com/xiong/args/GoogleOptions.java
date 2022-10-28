package com.xiong.args;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

public class GoogleOptions extends OptionsBase {
    @Option(name = "host",
            abbrev = 'h',//简写
            help = "主机host",
            category = "连接信息",
            defaultValue = "9870")
    public String host;
}
