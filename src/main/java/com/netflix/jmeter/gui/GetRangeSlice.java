package com.netflix.jmeter.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractSampler;
import com.netflix.jmeter.sampler.GetRangeSliceSampler;

public class GetRangeSlice extends AbstractGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    private static final String LABEL = "Cassandra Get Range Slice";
    private JTextField START_COLUMN_NAME;
    private JTextField END_COLUMN_NAME;
    private JTextField COUNT;
    private JCheckBox IS_REVERSE;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        START_COLUMN_NAME.setText(element.getPropertyAsString(GetRangeSliceSampler.START_COLUMN_NAME));
        END_COLUMN_NAME.setText(element.getPropertyAsString(GetRangeSliceSampler.END_COLUMN_NAME));
        COUNT.setText(element.getPropertyAsString(GetRangeSliceSampler.COUNT));
        IS_REVERSE.setSelected(element.getPropertyAsBoolean(GetRangeSliceSampler.IS_REVERSE));
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
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setStartName(START_COLUMN_NAME.getText());
            gSampler.setEndName(END_COLUMN_NAME.getText());
            gSampler.setCount(COUNT.getText());
            gSampler.setReverse(IS_REVERSE.isSelected());
        }
    }

    public void initFields()
    {
        START_COLUMN_NAME.setText("${__Random(1,1000)}");
        END_COLUMN_NAME.setText("${__Random(1,1000)}");
        COUNT.setText("100");
        IS_REVERSE.setSelected(true);
        CSERIALIZER.setSelectedItem("StringSerializer");
        VSERIALIZER.setSelectedItem("StringSerializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Start Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, START_COLUMN_NAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("End Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, END_COLUMN_NAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Count: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, COUNT = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Reverse: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, IS_REVERSE = new JCheckBox());
        addToPanel(mainPanel, labelConstraints, 0, 7, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 7, CSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 8, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 8, VSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
    }

    @Override
    public String getLable()
    {
        return LABEL;
    }
}
