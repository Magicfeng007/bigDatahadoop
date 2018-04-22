package com.magic.hadoop.app.fs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @Author: Magicfeng007
 * @Description:
 * @Date: Created in 2018-04-22---下午9:27
 */
public class RegexExcludePathFilter implements PathFilter {
    private String regex;
    public RegexExcludePathFilter(String regex){
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
