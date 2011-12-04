package com.netflix.jmeter.gui;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractCassandraSampler;
import com.netflix.jmeter.sampler.GetRangeSliceSampler;

public class GetRangeSliceCassandraGUI extends AbstractCassandraGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    private static final String LABEL = "Cassandra Get Range Slice";
    private JTextField KEY;
    private JTextField START_COLUMN_NAME;
    private JTextField END_COLUMN_NAME;
    private JTextField COUNT;
    private JCheckBox IS_REVERSE;
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;

    @Override
    public String getStaticLabel()
    {
        return LABEL;
    }
    
    public String getLabelResource()
    {
        return LABEL;
    }
    
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        KEY.setText(element.getPropertyAsString(GetRangeSliceSampler.KEY));
        START_COLUMN_NAME.setText(element.getPropertyAsString(GetRangeSliceSampler.START_COLUMN_NAME));
        END_COLUMN_NAME.setText(element.getPropertyAsString(GetRangeSliceSampler.END_COLUMN_NAME));
        COUNT.setText(element.getPropertyAsString(GetRangeSliceSampler.COUNT));
        IS_REVERSE.setSelected(element.getPropertyAsBoolean(GetRangeSliceSampler.IS_REVERSE));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(GetRangeSliceSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(GetRangeSliceSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(GetRangeSliceSampler.VALUE_SERIALIZER_TYPE));
    }

    public TestElement createTestElement()
    {
        GetRangeSliceSampler sampler = new GetRangeSliceSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof GetRangeSliceSampler)
        {
            GetRangeSliceSampler gSampler = (GetRangeSliceSampler) sampler;
            gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setStartName(START_COLUMN_NAME.getText());
            gSampler.setEndName(END_COLUMN_NAME.getText());
            gSampler.setCount(COUNT.getText());
            gSampler.setReverse(IS_REVERSE.isSelected());
            gSampler.setKey(KEY.getText());
        }
    }

    public void initFields()
    {

        KEY.setText("${__Random(1,1000)}");
        START_COLUMN_NAME.setText("${__Random(1,1000)}");
        END_COLUMN_NAME.setText("${__Random(1,1000)}");
        COUNT.setText("100");
        IS_REVERSE.setSelected(true);
        KSERIALIZER.setSelectedItem("StringSerializer");
        CSERIALIZER.setSelectedItem("StringSerializer");
        VSERIALIZER.setSelectedItem("StringSerializer");
    }

    public void init()
    {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());
        JPanel mainPanel = new JPanel(new GridBagLayout());
        GridBagConstraints labelConstraints = new GridBagConstraints();
        labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

        GridBagConstraints editConstraints = new GridBagConstraints();
        editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
        editConstraints.weightx = 1.0;
        editConstraints.fill = GridBagConstraints.HORIZONTAL;

        addToPanel(mainPanel, labelConstraints, 0, 0, new JLabel("ROW KEY: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 0, KEY = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 1, new JLabel("START COLUMN NAME: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 1, START_COLUMN_NAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 2, new JLabel("END COLUMN NAME: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 2, END_COLUMN_NAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("COUNT: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, COUNT = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("REVERSE: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, IS_REVERSE = new JCheckBox());
        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, KSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, CSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 7, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 7, VSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));

        JPanel container = new JPanel(new BorderLayout());
        container.add(mainPanel, BorderLayout.NORTH);
        add(container, BorderLayout.CENTER);
    }
}
