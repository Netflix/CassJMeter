package com.netflix.jmeter.gui;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractCassandraSampler;
import com.netflix.jmeter.sampler.GetSampler;

public class GetCassandraGUI extends AbstractCassandraGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    public static String LABEL = "Cassandra Get";
    private JTextField KEY;
    private JTextField CNAME;
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
        KEY.setText(element.getPropertyAsString(GetSampler.KEY));
        CNAME.setText(element.getPropertyAsString(GetSampler.COLUMN_NAME));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(GetSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(GetSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(GetSampler.VALUE_SERIALIZER_TYPE));
    }

    public TestElement createTestElement()
    {
        GetSampler sampler = new GetSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof GetSampler)
        {
            GetSampler gSampler = (GetSampler) sampler;
            gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setColumnName(CNAME.getText());
            gSampler.setKey(KEY.getText());
        }
    }

    public void initFields()
    {

        KEY.setText("${__Random(1,1000)}");
        CNAME.setText("${__Random(1,1000)}");
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
        addToPanel(mainPanel, labelConstraints, 0, 1, new JLabel("COLUMN NAME: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 1, CNAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 2, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 2, KSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, CSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, VSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));

        JPanel container = new JPanel(new BorderLayout());
        container.add(mainPanel, BorderLayout.NORTH);
        add(container, BorderLayout.CENTER);
    }
}
