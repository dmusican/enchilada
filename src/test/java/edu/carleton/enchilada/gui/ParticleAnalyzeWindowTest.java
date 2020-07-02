package edu.carleton.enchilada.gui;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class ParticleAnalyzeWindowTest {

    @Test
    public void buildBothLabelSigs() throws FileNotFoundException {
        ParticleAnalyzeWindow.buildBothLabelSigs(new ArrayList<>(), new ArrayList<>());
    }

    @Test
    public void writeBothSpectra() throws FileNotFoundException {
        ParticleAnalyzeWindow.writeBothSpectra(new ArrayList<>(), new ArrayList<>());
    }

    @Test
    public void copyLabelingFilesOutOfResources() {
        ParticleAnalyzeWindow.copyLabelingFilesOutOfResources();
        String[] filesToCopy = {"pion-sigs.txt", "nion-sigs.txt", "run.bat", "spectrum.exe"};
        for (String fileToCopy : filesToCopy) {
            assertTrue(ParticleAnalyzeWindow.labelingDir.resolve(fileToCopy).toFile().exists());
        }
    }

    @Test
    public void getLabelingProcess() throws IOException {
        ParticleAnalyzeWindow.getLabelingProcess();
    }
}