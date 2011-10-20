package com.netflix.jmeter.gui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;

import org.apache.jmeter.samplers.gui.AbstractSamplerGui;

public abstract class AbstractCassandraGUI extends AbstractSamplerGui
{
    private static final long serialVersionUID = -1372154378991423872L;
    
    public AbstractCassandraGUI()
    {
        init();
    }
    
    @Override
    public void clearGui()
    {
        super.clearGui();
        initFields();
    }
    
    public static Component addHelpLinkToPanel(Container panel, String helpPage)
    {
        if (!java.awt.Desktop.isDesktopSupported())
        {
            return panel;
        }

        JLabel icon = new JLabel();
        JLabel link = new JLabel("Help on this plugin");
        link.setForeground(Color.blue);
        link.setFont(link.getFont().deriveFont(Font.PLAIN));
        link.setCursor(new Cursor(Cursor.HAND_CURSOR));
        Border border = BorderFactory.createMatteBorder(0, 0, 1, 0, java.awt.Color.blue);
        link.setBorder(border);

        JLabel version = new JLabel("v" + 123);
        version.setFont(version.getFont().deriveFont(Font.PLAIN).deriveFont(11F));
        version.setForeground(Color.GRAY);

        JPanel panelLink = new JPanel(new GridBagLayout());

        GridBagConstraints gridBagConstraints;

        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.insets = new java.awt.Insets(0, 1, 0, 0);
        panelLink.add(icon, gridBagConstraints);

        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.weightx = 1.0;
        gridBagConstraints.insets = new java.awt.Insets(0, 2, 3, 0);
        panelLink.add(link, gridBagConstraints);

        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 2;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.insets = new java.awt.Insets(0, 0, 0, 4);
        panelLink.add(version, gridBagConstraints);
        panel.add(panelLink);
        return panel;
    }

    public void addToPanel(JPanel panel, GridBagConstraints constraints, int col, int row, JComponent component)
    {
        constraints.gridx = col;
        constraints.gridy = row;
        panel.add(component, constraints);
    }
    public abstract void init();
    public abstract void initFields();
}
