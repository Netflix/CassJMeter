package com.netflix.jmeter.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.Border;

import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractSampler;
import com.netflix.jmeter.sampler.Connection;

public abstract class AbstractGUI extends AbstractSamplerGui
{
    private static final long serialVersionUID = -1372154378991423872L;
    private static final String WIKI = "https://github.com/Netflix/CassJMeter";
    private JTextField KEY;
    private JComboBox KSERIALIZER;
    private JTextField COLUMN_FAMILY;

    public AbstractGUI()
    {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());
        add(addHelpLinkToPanel(makeTitlePanel(), WIKI), BorderLayout.NORTH);
        JPanel mainPanel = new JPanel(new GridBagLayout());
        GridBagConstraints labelConstraints = new GridBagConstraints();
        labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

        GridBagConstraints editConstraints = new GridBagConstraints();
        editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
        editConstraints.weightx = 1.0;
        editConstraints.fill = GridBagConstraints.HORIZONTAL;
        
        addToPanel(mainPanel, labelConstraints, 0, 1, new JLabel("Column Family: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 1, COLUMN_FAMILY = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 2, new JLabel("Row Key: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 2, KEY = new JTextField());
        init(mainPanel, labelConstraints, editConstraints);
        
        addToPanel(mainPanel, labelConstraints, 0, 10, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 10, KSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        JPanel container = new JPanel(new BorderLayout());
        container.add(mainPanel, BorderLayout.NORTH);
        add(container, BorderLayout.CENTER);
    }

    @Override
    public void clearGui()
    {
        super.clearGui();
        KEY.setText("${__Random(1,1000)}");
        KSERIALIZER.setSelectedItem("StringSerializer");
        COLUMN_FAMILY.setText("Standard3");
        initFields();
        if (Connection.connection != null)
        {
        	Connection.getInstance().shutdown();
        }
    }

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        KEY.setText(element.getPropertyAsString(AbstractSampler.KEY));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(AbstractSampler.KEY_SERIALIZER_TYPE));
        COLUMN_FAMILY.setText(element.getPropertyAsString(AbstractSampler.COLUMN_FAMILY));
    }
    
    protected void configureTestElement(TestElement mc) 
    {
        super.configureTestElement(mc);
        if (mc instanceof AbstractSampler)
        {
            AbstractSampler gSampler = (AbstractSampler) mc;
            gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setKey(KEY.getText());
            gSampler.setColumnFamily(COLUMN_FAMILY.getText());
        }
    }
    
    public static Component addHelpLinkToPanel(Container panel, String helpPage)
    {
        if (!java.awt.Desktop.isDesktopSupported())
            return panel;

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

    @Override
    public String getStaticLabel()
    {
        return getLable();
    }

    @Override
    public String getLabelResource()
    {
        return getLable();
    }

    public abstract String getLable();

    public abstract void initFields();

    public abstract void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints);
}
