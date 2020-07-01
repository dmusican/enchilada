package edu.carleton.enchilada.gui;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class ParticleAnalyzeWindowTest {

    @Test
    public void buildBothLabelSigs() throws FileNotFoundException {
        ParticleAnalyzeWindow.buildBothLabelSigs(new ArrayList<>(), new ArrayList<>());
    }
}